package drill

import (
	"errors"
	"fmt"
	"io"

	"github.com/factset/go-drill/internal/data"
	"github.com/factset/go-drill/internal/rpc/proto/common"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
)

type DataVector data.DataVector
type NullableDataVector data.NullableDataVector

// A DataHandler is an object that allows iterating through record batches as the data
// comes in, or cancelling a running query.
type DataHandler interface {
	Next() (RowBatch, error)
	Cancel()
	GetCols() []string
	GetRecordBatch() RowBatch
	Close() error
}

type RowBatch interface {
	NumCols() int
	NumRows() int32
	AffectedRows() int32
	IsNullable(index int) bool
	TypeName(index int) string
	PrecisionScale(index int) (precision, scale int64, ok bool)
	GetVectors() []DataVector
}

// A ResultHandle is an opaque handle for a given result set, implementing the DataHandler
// interface.
//
// It contains a channel over which the client will send the data as it comes in
// allowing results to be streamed as they are retrieved. It also contains a
// handle to the original client that the result came from.
type ResultHandle struct {
	dataChannel chan *queryData
	queryResult *shared.QueryResult
	curBatch    *recordBatch

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

// A recordBatch represents the data and meta information for one group of rows.
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
type recordBatch struct {
	def  *shared.RecordBatchDef
	vecs []DataVector
}

func (rb *recordBatch) GetVectors() []DataVector {
	return rb.vecs
}

func (rb *recordBatch) AffectedRows() int32 {
	return rb.def.GetAffectedRowsCount()
}

func (rb *recordBatch) NumCols() int {
	return len(rb.def.Field)
}

func (rb *recordBatch) NumRows() int32 {
	return rb.def.GetRecordCount()
}

func (rb *recordBatch) IsNullable(index int) bool {
	return rb.def.Field[index].MajorType.GetMode() == common.DataMode_OPTIONAL
}

func (rb *recordBatch) TypeName(index int) string {
	return rb.def.Field[index].MajorType.GetMinorType().String()
}

func (rb *recordBatch) PrecisionScale(index int) (precision, scale int64, ok bool) {
	typ := rb.def.Field[index].GetMajorType()
	switch typ.GetMinorType() {
	case common.MinorType_DECIMAL9,
		common.MinorType_DECIMAL18,
		common.MinorType_DECIMAL28SPARSE,
		common.MinorType_DECIMAL38SPARSE,
		common.MinorType_MONEY,
		common.MinorType_FLOAT4,
		common.MinorType_FLOAT8,
		common.MinorType_DECIMAL28DENSE,
		common.MinorType_DECIMAL38DENSE:

		precision = int64(typ.GetPrecision())
		scale = int64(typ.GetScale())
		ok = true
	}
	return
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
func (r *ResultHandle) GetRecordBatch() RowBatch {
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

	cols := make([]string, len(r.curBatch.def.GetField()))
	for idx, f := range r.curBatch.def.GetField() {
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
func (r *ResultHandle) Next() (RowBatch, error) {
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

	switch q.typ {
	case int32(user.RpcType_QUERY_DATA):
		// more data!
		qd := q.msg.(*shared.QueryData)
		r.curBatch = &recordBatch{
			vecs: make([]DataVector, 0, len(qd.GetDef().GetField())),
			def:  qd.GetDef(),
		}

		var offset int32 = 0
		for _, f := range qd.GetDef().GetField() {
			r.curBatch.vecs = append(r.curBatch.vecs, data.NewValueVec(q.raw[offset:offset+f.GetBufferLength()], f))
			offset += f.GetBufferLength()
		}
	case int32(user.RpcType_QUERY_RESULT):
		// we got our query result, store it!
		r.queryResult = q.msg.(*shared.QueryResult)
	}
}
