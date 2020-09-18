package drill

import (
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/factset/go-drill/internal/data"
	"github.com/factset/go-drill/internal/rpc/proto/common"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
)

type DataVector data.DataVector
type NullableDataVector data.NullableDataVector

type dataImplType int

const (
	basicData dataImplType = iota
	arrowData
)

// A DataHandler is an object that allows iterating through record batches as the data
// comes in, or cancelling a running query.
type DataHandler interface {
	Next() (RowBatch, error)
	Cancel()
	GetCols() []string
	GetRecordBatch() RowBatch
	Close() error
}

// RowBatch presents the interface for dealing with record batches so that we don't
// have to expose the object and can swap out our underlying implementation in the
// future if needed.
type RowBatch interface {
	NumCols() int
	NumRows() int32
	AffectedRows() int32
	IsNullable(index int) bool
	TypeName(index int) string
	PrecisionScale(index int) (precision, scale int64, ok bool)
	GetVectors() []DataVector
	ColumnName(i int) string
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
	curBatch    RowBatch

	implType dataImplType

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
// This object isn't exposed directly in order to hide the Protobuf implementation
// details from consumers, instead we expose an interface which allows function calls
// to expose the desired information.
type recordBatch struct {
	def  *shared.RecordBatchDef
	vecs []DataVector
}

func newRecordBatch(qd *shared.QueryData, raw []byte) RowBatch {
	batch := &recordBatch{
		vecs: make([]DataVector, 0, len(qd.GetDef().GetField())),
		def:  qd.GetDef(),
	}

	var offset int32 = 0
	for _, f := range qd.GetDef().GetField() {
		batch.vecs = append(batch.vecs, data.NewValueVec(raw[offset:offset+f.GetBufferLength()], f))
		offset += f.GetBufferLength()
	}
	return batch
}

func (rb *recordBatch) ColumnName(i int) string {
	return rb.def.Field[i].NamePart.GetName()
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

// ArrowVectorWrapper is a thin wrapper around an arrow array to fit our DataVector
// interface so that we can use arrow Records as the underlying data storage for
// our record handling if desired. This allows future enhancements and also
// letting our current set up for returning data be allowed to return arrow records
// without having to change much.
type ArrowVectorWrapper struct {
	array.Interface
}

// Value for now will always return nil, the expectation is to cast the wrapper
// to an array.Interface and use it directly via arrow rather than using the DataVector
// interface.
//
// TODO: implement type switch from arrow to pull value for this
func (a ArrowVectorWrapper) Value(index uint) interface{} {
	// for now let's not bother with this for arrow and just have consumers
	// cast to the underlying arrow vector
	return nil
}

// TypeLen returns the max length for variable length types (the second output is true)
// otherwise fixed length types return false for the second output.
func (a ArrowVectorWrapper) TypeLen() (int64, bool) {
	switch a.DataType().ID() {
	case arrow.BINARY, arrow.STRING:
		return int64(math.MaxUint16), true
	default:
		return 0, false
	}
}

// GetNullBytemap returns the bytes of the bitmap from the underlying arrow array.
func (a ArrowVectorWrapper) GetNullBytemap() []byte {
	return a.NullBitmapBytes()
}

// GetRawBytes returns the underlying raw data for the array.
func (a ArrowVectorWrapper) GetRawBytes() []byte {
	switch a.DataType().ID() {
	case arrow.BINARY, arrow.STRING, arrow.FIXED_SIZE_BINARY:
		return a.Data().Buffers()[2].Bytes()
	default:
		return a.Data().Buffers()[1].Bytes()
	}
}

// IsNull just forwards to the underlying IsNull for the arrow array.
func (a ArrowVectorWrapper) IsNull(index uint) bool {
	return a.Interface.IsNull(int(index))
}

// Type determines the go reflection type for the values of the array.
func (a ArrowVectorWrapper) Type() reflect.Type {
	return data.ArrowTypeToReflect(a.DataType())
}

// ArrowBatch is a record batch implementation of RowBatch which uses an arrow
// array.Record as the underlying storage instead.
type ArrowBatch struct {
	array.Record
}

func newArrowBatch(qd *shared.QueryData, raw []byte) RowBatch {
	fields := make([]arrow.Field, len(qd.GetDef().GetField()))
	cols := make([]array.Interface, len(qd.GetDef().GetField()))

	var offset int32 = 0
	for idx, f := range qd.GetDef().GetField() {
		fields[idx].Name = f.NamePart.GetName()
		fields[idx].Nullable = f.MajorType.GetMode() == common.DataMode_OPTIONAL

		cols[idx] = data.NewArrowArray(raw[offset:offset+f.GetBufferLength()], f)
		fields[idx].Type = cols[idx].DataType()
		fields[idx].Metadata = arrow.NewMetadata([]string{"dbtype"}, []string{f.MajorType.MinorType.String()})
		offset += f.GetBufferLength()
	}

	metadata := arrow.NewMetadata([]string{"affectedRows"}, []string{strconv.Itoa(int(qd.GetAffectedRowsCount()))})
	schema := arrow.NewSchema(fields, &metadata)
	return ArrowBatch{array.NewRecord(schema, cols, int64(qd.Def.GetRecordCount()))}
}

// NumCols just returns the number of columns in the row batch.
func (a ArrowBatch) NumCols() int {
	return int(a.Record.NumCols())
}

// NumRows returns the number of rows in the rowbatch
func (a ArrowBatch) NumRows() int32 {
	return int32(a.Record.NumRows())
}

// AffectedRows returns the number of rows listed as "Affected" by the response
// typically 0 unless the query was an insert/update.
func (a ArrowBatch) AffectedRows() (rows int32) {
	meta := a.Schema().Metadata()
	idx := meta.FindKey("affectedRows")
	if idx == -1 {
		return
	}

	val, err := strconv.Atoi(meta.Values()[idx])
	if err != nil {
		return
	}
	rows = int32(val)
	return
}

// IsNullable reports whether the column index is a vector that can be null.
func (a ArrowBatch) IsNullable(index int) bool {
	return a.Schema().Field(index).Nullable
}

// TypeName returns the Database Type string for the column as defined by the
// record definition from drill.
func (a ArrowBatch) TypeName(index int) string {
	field := a.Schema().Field(index)
	idx := field.Metadata.FindKey("dbtype")
	if idx == -1 {
		return strings.ToUpper(field.Type.Name())
	}

	return field.Metadata.Values()[idx]
}

// PrecisionScale returns the precision and scale of the type for this column as
// defined in the RowsColumnTypePrecisionScale interface of database/sql/driver
func (a ArrowBatch) PrecisionScale(index int) (precision, scale int64, ok bool) {
	field := a.Schema().Field(index)
	if field.Type.ID() != arrow.DECIMAL {
		return
	}

	precision = int64(field.Type.(*arrow.Decimal128Type).Precision)
	scale = int64(field.Type.(*arrow.Decimal128Type).Scale)
	ok = true
	return
}

// GetVectors returns the actual columns of data for this row batch.
func (a ArrowBatch) GetVectors() []DataVector {
	ret := make([]DataVector, a.NumCols())
	for idx, arr := range a.Columns() {
		ret[idx] = ArrowVectorWrapper{arr}
	}
	return ret
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

	cols := make([]string, r.curBatch.NumCols())
	for i := 0; i < r.curBatch.NumCols(); i++ {
		cols[i] = r.curBatch.ColumnName(i)
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
		switch r.implType {
		case basicData:
			r.curBatch = newRecordBatch(qd, q.raw)
		case arrowData:
			r.curBatch = newArrowBatch(qd, q.raw)
		}
	case int32(user.RpcType_QUERY_RESULT):
		// we got our query result, store it!
		r.queryResult = q.msg.(*shared.QueryResult)
	}
}
