package data

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/factset/go-drill/internal/rpc/proto/common"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
)

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
