package drill

import (
	"errors"
	"fmt"
	"io"

	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
)

// A ResultHandle is an opaque handle for a given result set.
//
// It contains a channel over which the client will send the data as it comes in
// allowing results to be streamed as they are retrieved. It also contains a
// handle to the original client that the result came from.
type ResultHandle struct {
	dataChannel chan *queryData
	queryResult *shared.QueryResult
	curBatch    *RecordBatch

	queryID *shared.QueryId
	client  *Client
}

// A PreparedHandle is an opaque handle for a Prepared Statement.
//
// This does not contain a reference to the client it originally came from as it
// can be passed to any valid drill client in order to execute it as long as it
// is connected to the same server cluster. This is because the prepared statement
// information is stored on the server, this object contains the server handle
// needed to execute the statement.
//
// Keep in mind that Apache Drill *does not* support parameters in prepared statements
type PreparedHandle interface{}

// A RecordBatch represents the data and meta information for one group of rows.
//
// How to Interpret
//
// The Vecs are the columns in a result set as Drill returns data in a column oriented
// fashion. Each datavector has been processed from the results Drill returned and
// may or may not be able to contain nulls depending on whether or not the column was
// considered OPTIONAL. Every vector can return its reflect.Type, and can retrieve data
// for an index as an interface{}. To get the raw value without needing an interface
// you'll need to cast away from the DataVector interface to the underlying type.
//
// The Def object describes the structure of this batch, giving the Record Count,
// or the AffectedRowsCount. It also allows access to the meta data for the serialized
// fields, as per the Protobuf definitions. Through the fields we can get the types,
// the column names, and information about how it was serialized.
//
// Future Looking
//
// Eventually this will likely hide the protobuf implementation behind an interface,
// but for now it was easier to just expose the protobuf definitions.
type RecordBatch struct {
	Def  *shared.RecordBatchDef
	Vecs []data.DataVector
}

// Close the channel and remove the query handler from the client
func (r *ResultHandle) Close() error {
	close(r.dataChannel)
	r.client.resultMap.Delete(qid{*r.queryID.Part1, *r.queryID.Part2})
	return nil
}

// GetRecordBatch will return the current record batch, if we have not yet
// recieved one it will attempt to check the channel and block for the first
// batch. If this returns nil then that means there is no data and calling
// Next will return the status.
func (r *ResultHandle) GetRecordBatch() *RecordBatch {
	if r.curBatch == nil {
		r.nextBatch()
	}

	return r.curBatch
}

// GetCols grabs the column names from the record batch definition.
//
// This will potentially block on the data channel if we have not yet recieved
// the first record batch. If the query failed or otherwise then this will return
// an empty slice.
func (r *ResultHandle) GetCols() []string {
	if r.curBatch == nil {
		r.nextBatch()
	}

	cols := make([]string, len(r.curBatch.Def.GetField()))
	for idx, f := range r.curBatch.Def.GetField() {
		cols[idx] = *f.NamePart.Name
	}

	return cols
}

// Cancel will cancel the currently calculating query. Calls to Next can still
// be used to drain any remaining record batches.
func (r *ResultHandle) Cancel() {
	r.client.sendCancel(r.queryID)
}

// These error types are potentially returned by calls to Next. If
// the query fails, the ErrQueryFailed type will be wrapped by the returned
// error, allowing usage like so:
//
//   err := handle.Next()
//   if errors.Is(err, ErrQueryFailed) {
//     err.Error() contains the actual error message
//     // handle query failure
//   }
var (
	// This is returned when we get a query result status of Cancelled
	ErrQueryCancelled = errors.New("drill: query cancelled")
	// This is wrapped by the returned error if the query failed
	ErrQueryFailed = errors.New("drill: query failed")
	// This is returned if the query failed for some unknown reason
	ErrQueryUnknownState = errors.New("drill: query unknown state")
)

// Next checks the data channel for the next record batch, dropping
// the reference to the current record batch.
//
// Returns
//
// Next will return nil if it is successful in retrieving another record batch.
// Otherwise it will return io.EOF if the query completed successfully and
// there are no more record batches.
//
// If there are no more record batches and the query did not complete successfully,
// it will return either an error wrapping ErrQueryFailed, or one of the other
// error types.
func (r *ResultHandle) Next() (*RecordBatch, error) {
	r.curBatch = nil
	r.nextBatch()
	if r.curBatch != nil {
		return r.curBatch, nil
	}

	if r.queryResult == nil {
		return nil, ErrQueryUnknownState
	}

	switch r.queryResult.GetQueryState() {
	case shared.QueryResult_COMPLETED:
		return nil, io.EOF
	case shared.QueryResult_CANCELED:
		return nil, ErrQueryCancelled
	case shared.QueryResult_FAILED:
		return nil, fmt.Errorf("%w: %s", ErrQueryFailed, r.queryResult.GetError()[0].GetMessage())
	default:
		return nil, ErrQueryUnknownState
	}
}

func (r *ResultHandle) nextBatch() {
	// check the channel for data or potentially being closed
	q, ok := <-r.dataChannel
	if !ok {
		// if the channel is closed then there's nothing more we can do here
		return
	}

	// b, _ := proto.Marshal(q.msg)
	// fmt.Println(q.typ)
	// fmt.Println(q.msg)
	// fmt.Println(hex.EncodeToString(b))
	// fmt.Println(hex.EncodeToString(q.raw))

	switch q.typ {
	case int32(user.RpcType_QUERY_DATA):
		// more data!
		qd := q.msg.(*shared.QueryData)
		r.curBatch = &RecordBatch{
			Vecs: make([]data.DataVector, 0, len(qd.GetDef().GetField())),
			Def:  qd.GetDef(),
		}

		var offset int32 = 0
		for _, f := range qd.GetDef().GetField() {
			r.curBatch.Vecs = append(r.curBatch.Vecs, data.NewValueVec(q.raw[offset:offset+f.GetBufferLength()], f))
			offset += f.GetBufferLength()
		}
	case int32(user.RpcType_QUERY_RESULT):
		// we got our query result, store it!
		r.queryResult = q.msg.(*shared.QueryResult)
	}
}
