package data

//go:generate go run ../cmd/tmpl -data numeric.tmpldata arrow_numeric.gen_test.go.tmpl

import (
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/factset/go-drill/internal/rpc/proto/common"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
)

// ArrowTypeToReflect will get the reflection type from the arrow datatype.
//
// TODO: handle decimal types properly
func ArrowTypeToReflect(typ arrow.DataType) reflect.Type {
	switch typ.ID() {
	case arrow.BOOL:
		return reflect.TypeOf(true)
	case arrow.UINT8:
		return reflect.TypeOf(uint8(0))
	case arrow.INT8:
		return reflect.TypeOf(int8(0))
	case arrow.UINT16:
		return reflect.TypeOf(uint16(0))
	case arrow.INT16:
		return reflect.TypeOf(int16(0))
	case arrow.UINT32:
		return reflect.TypeOf(uint32(0))
	case arrow.INT32:
		return reflect.TypeOf(int32(0))
	case arrow.UINT64:
		return reflect.TypeOf(uint64(0))
	case arrow.INT64:
		return reflect.TypeOf(int64(0))
	case arrow.FLOAT32:
		return reflect.TypeOf(float32(0))
	case arrow.FLOAT64:
		return reflect.TypeOf(float64(0))
	case arrow.STRING:
		return reflect.TypeOf("")
	case arrow.BINARY, arrow.FIXED_SIZE_BINARY:
		return reflect.TypeOf([]byte{})
	case arrow.TIMESTAMP:
		return reflect.TypeOf(arrow.Timestamp(0))
	case arrow.DATE64:
		return reflect.TypeOf(arrow.Date64(0))
	case arrow.TIME32:
		return reflect.TypeOf(arrow.Time32(0))
	case arrow.INTERVAL:
		switch typ.(type) {
		case *arrow.DayTimeIntervalType:
			return reflect.TypeOf(arrow.DayTimeInterval{})
		case *arrow.MonthIntervalType:
			return reflect.TypeOf(arrow.MonthInterval(0))
		}
	case arrow.DECIMAL:
		return reflect.TypeOf(decimal128.FromI64(0))
	}
	return nil
}

// TypeToArrowType converts the specified type enum to an arrow Data Type
//
// TODO: handle decimal types
func TypeToArrowType(typ common.MinorType) arrow.DataType {
	switch typ {
	case common.MinorType_BIGINT:
		return arrow.PrimitiveTypes.Int64
	case common.MinorType_INT:
		return arrow.PrimitiveTypes.Int32
	case common.MinorType_SMALLINT:
		return arrow.PrimitiveTypes.Int16
	case common.MinorType_TINYINT:
		return arrow.PrimitiveTypes.Int8
	case common.MinorType_DATE:
		return arrow.FixedWidthTypes.Date64
	case common.MinorType_TIME:
		return arrow.FixedWidthTypes.Time32ms
	case common.MinorType_BIT:
		return arrow.FixedWidthTypes.Boolean
	case common.MinorType_FLOAT4:
		return arrow.PrimitiveTypes.Float32
	case common.MinorType_FLOAT8:
		return arrow.PrimitiveTypes.Float64
	case common.MinorType_UINT1:
		return arrow.PrimitiveTypes.Uint8
	case common.MinorType_UINT2:
		return arrow.PrimitiveTypes.Uint16
	case common.MinorType_UINT4:
		return arrow.PrimitiveTypes.Uint32
	case common.MinorType_UINT8:
		return arrow.PrimitiveTypes.Uint64
	case common.MinorType_INTERVALDAY:
		return arrow.FixedWidthTypes.DayTimeInterval
	case common.MinorType_INTERVALYEAR:
		return arrow.FixedWidthTypes.MonthInterval
	case common.MinorType_VARCHAR:
		return arrow.BinaryTypes.String
	case common.MinorType_VARBINARY:
		return arrow.BinaryTypes.Binary
	case common.MinorType_TIMESTAMP:
		return arrow.FixedWidthTypes.Timestamp_ms
	}
	return arrow.Null
}

func nullBytesToBits(bytemap []byte) []byte {
	ret := make([]byte, bitutil.CeilByte(len(bytemap)))
	for idx, b := range bytemap {
		if b != 0 {
			bitutil.SetBit(ret, idx)
		}
	}
	return ret
}

// NewArrowArray constructs an arrow.Interface array from the given raw data and serialized
// metadata as a zero-copy array.
//
// TODO: Handle decimal types properly
func NewArrowArray(rawData []byte, meta *shared.SerializedField) (ret array.Interface) {
	arrowType := TypeToArrowType(meta.GetMajorType().GetMinorType())
	if arrowType == arrow.Null {
		return array.NewNull(int(meta.GetValueCount()))
	}

	fieldMeta := meta
	remaining := rawData
	buffers := make([]*memory.Buffer, 1, 2)
	nullCount := array.UnknownNullCount
	if meta.GetMajorType().GetMode() == common.DataMode_OPTIONAL {
		buffers[0] = memory.NewBufferBytes(nullBytesToBits(rawData[:meta.GetValueCount()]))
		remaining = rawData[meta.GetValueCount():]
		fieldMeta = meta.Child[1]
	} else {
		buffers[0] = nil
		nullCount = 0
	}

	if len(fieldMeta.Child) > 0 && fieldMeta.Child[0].NamePart.GetName() == "$offsets$" {
		buffers = append(buffers, memory.NewBufferBytes(remaining[:fieldMeta.Child[0].GetBufferLength()]))
		remaining = remaining[fieldMeta.Child[0].GetBufferLength():]
	}

	buffers = append(buffers, memory.NewBufferBytes(remaining))

	data := array.NewData(arrowType, int(meta.GetValueCount()), buffers, nil, nullCount, 0)
	switch arrowType.(type) {
	case *arrow.DayTimeIntervalType, *arrow.MonthIntervalType:
		ret = array.NewIntervalData(data)
	default:
		ret = array.MakeFromData(data)
	}
	data.Release()
	return
}
