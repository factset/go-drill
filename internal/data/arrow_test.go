package data_test

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/factset/go-drill/internal/data"
	"github.com/factset/go-drill/internal/rpc/proto/common"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func createMetaField(mode common.DataMode, typ *common.MinorType, count, bufflen int32) *shared.SerializedField {
	if mode == common.DataMode_REQUIRED {
		return &shared.SerializedField{
			MajorType: &common.MajorType{
				MinorType: typ,
				Mode:      common.DataMode_REQUIRED.Enum(),
			},
			ValueCount:   proto.Int32(count),
			BufferLength: proto.Int32(bufflen),
		}
	}

	return &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: typ,
			Mode:      common.DataMode_OPTIONAL.Enum(),
		},
		Child: []*shared.SerializedField{
			{
				MajorType: &common.MajorType{
					MinorType: common.MinorType_UINT1.Enum(),
					Mode:      common.DataMode_REQUIRED.Enum(),
				},
				NamePart:     &shared.NamePart{Name: proto.String("$bits$")},
				ValueCount:   proto.Int32(count),
				BufferLength: proto.Int32(count),
			},
			{
				MajorType: &common.MajorType{
					MinorType: typ,
					Mode:      common.DataMode_REQUIRED.Enum(),
				},
				ValueCount:   proto.Int32(count),
				BufferLength: proto.Int32(bufflen),
			},
		},
		ValueCount:   proto.Int32(count),
		BufferLength: proto.Int32(count + bufflen),
	}
}

func TestBitArrow(t *testing.T) {
	const N = 15

	meta := createMetaField(common.DataMode_REQUIRED, common.MinorType_BIT.Enum(), int32(N), int32(bitutil.CeilByte(N)))

	arr := data.NewArrowArray([]byte{0xaa, 0xaa}, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.Boolean, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(false), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, N, arr.Len())
	assert.Zero(t, arr.NullN())

	for i := 0; i < N; i++ {
		assert.Exactly(t, i%2 == 1, arr.(*array.Boolean).Value(i))
	}
}

func TestOptionalBitArrow(t *testing.T) {
	const N = 15
	meta := createMetaField(common.DataMode_OPTIONAL, common.MinorType_BIT.Enum(), int32(N), int32(bitutil.CeilByte(N)))

	bytemap := make([]byte, N)
	for i := 0; i < N; i++ {
		bytemap[i] = byte(i % 2)
	}

	// 0xcc bit pattern is 11001100 so i%4== 0 and i%4==1 will be false and i%4==2 and i%4==3 will be true
	arr := data.NewArrowArray(append(bytemap, []byte{0xcc, 0xcc}...), meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.Boolean, arr.DataType())
	assert.Equal(t, N, arr.Len())
	assert.EqualValues(t, math.Ceil(float64(N)/2), arr.NullN())

	for i := 0; i < N; i++ {
		assert.Equal(t, i%2 == 1, arr.IsValid(i))
		assert.Equal(t, i%2 == 0, arr.IsNull(i))

		assert.Exactly(t, i%4 >= 2, arr.(*array.Boolean).Value(i))
	}
}

func TestBinaryArrow(t *testing.T) {
	vbytes, meta := getVarcharBytes()

	meta.MajorType = &common.MajorType{
		MinorType: common.MinorType_VARBINARY.Enum(),
		Mode:      common.DataMode_REQUIRED.Enum(),
	}

	arr := data.NewArrowArray(vbytes, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.BinaryTypes.Binary, arr.DataType())
	assert.Exactly(t, reflect.TypeOf([]byte{}), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, len(varchardata), arr.Len())

	for idx, val := range varchardata {
		assert.Equal(t, val, arr.(*array.Binary).ValueString(idx))
	}
}

func TestOptionalBinaryArrow(t *testing.T) {
	vbytes, vmeta := getVarcharBytes()

	bytemap := make([]byte, len(varchardata))
	for idx := range varchardata {
		bytemap[idx] = byte(idx % 2)
	}

	meta := createMetaField(common.DataMode_OPTIONAL, common.MinorType_VARBINARY.Enum(), int32(len(varchardata)), int32(len(vbytes)))
	meta.Child[1] = vmeta

	arr := data.NewArrowArray(append(bytemap, vbytes...), meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.BinaryTypes.Binary, arr.DataType())
	assert.Equal(t, len(varchardata), arr.Len())
	assert.EqualValues(t, math.Ceil(float64(len(varchardata))/2), arr.NullN())

	for idx, val := range varchardata {
		assert.Exactly(t, idx%2 == 0, arr.IsNull(idx))
		assert.Exactly(t, idx%2 == 1, arr.IsValid(idx))
		assert.Equal(t, val, arr.(*array.Binary).ValueString(idx))
	}
}

func TestStringArrow(t *testing.T) {
	vbytes, meta := getVarcharBytes()

	meta.MajorType = &common.MajorType{
		MinorType: common.MinorType_VARCHAR.Enum(),
		Mode:      common.DataMode_REQUIRED.Enum(),
	}

	arr := data.NewArrowArray(vbytes, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.BinaryTypes.String, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(""), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, len(varchardata), arr.Len())

	for idx, val := range varchardata {
		assert.Equal(t, val, arr.(*array.String).Value(idx))
	}
}

func TestOptionalStringArrow(t *testing.T) {
	vbytes, vmeta := getVarcharBytes()

	bytemap := make([]byte, len(varchardata))
	for idx := range varchardata {
		bytemap[idx] = byte(idx % 2)
	}

	meta := createMetaField(common.DataMode_OPTIONAL, common.MinorType_VARCHAR.Enum(), int32(len(varchardata)), int32(len(vbytes)))
	meta.Child[1] = vmeta

	arr := data.NewArrowArray(append(bytemap, vbytes...), meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.BinaryTypes.String, arr.DataType())
	assert.Equal(t, len(varchardata), arr.Len())
	assert.EqualValues(t, math.Ceil(float64(len(varchardata))/2), arr.NullN())

	for idx, val := range varchardata {
		assert.Exactly(t, idx%2 == 0, arr.IsNull(idx))
		assert.Exactly(t, idx%2 == 1, arr.IsValid(idx))
		assert.Equal(t, val, arr.(*array.String).Value(idx))
	}
}

func TestTimestampArrow(t *testing.T) {
	const N = 10
	values := make([]int64, N)
	tvalues := make([]time.Time, N)
	for idx := range values {
		tv := time.Now().Add(time.Duration(-1*idx) * time.Hour).Truncate(time.Second)
		values[idx] = tv.UnixNano() / 1000000
		tvalues[idx] = tv
	}

	b := data.Int64Traits.CastToBytes(values)
	arr := data.NewArrowArray(b, createMetaField(common.DataMode_REQUIRED, common.MinorType_TIMESTAMP.Enum(), int32(N), int32(len(b))))
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.Timestamp_ms, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(arrow.Timestamp(0)), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, N, arr.Len())

	for idx, val := range values {
		arrowVal := arr.(*array.Timestamp).Value(idx)
		assert.Equal(t, arrow.Timestamp(val), arrowVal)
		assert.True(t, tvalues[idx].Equal(time.Unix(int64(arrowVal)/1000, 0)))
		assert.Equal(t, tvalues[idx], time.Unix(int64(arrowVal)/1000, 0))
	}
}

func TestOptionalTimestampArrow(t *testing.T) {
	const N = 10
	values := make([]int64, N)
	tvalues := make([]time.Time, N)
	for idx := range values {
		tv := time.Now().Add(time.Duration(-1*idx) * time.Hour).Truncate(time.Second)
		values[idx] = tv.UnixNano() / 1000000
		tvalues[idx] = tv
	}

	b := data.Int64Traits.CastToBytes(values)
	bytemap := make([]byte, N)
	for i := 0; i < N; i++ {
		bytemap[i] = byte(i % 2)
	}

	arr := data.NewArrowArray(append(bytemap, b...), createMetaField(common.DataMode_OPTIONAL, common.MinorType_TIMESTAMP.Enum(), int32(N), int32(len(b))))
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.Timestamp_ms, arr.DataType())
	assert.Equal(t, N, arr.Len())
	assert.EqualValues(t, math.Ceil(float64(N)/2), arr.NullN())

	for idx, val := range values {
		assert.Exactly(t, idx%2 == 0, arr.IsNull(idx))
		assert.Exactly(t, idx%2 == 1, arr.IsValid(idx))

		arrowVal := arr.(*array.Timestamp).Value(idx)
		assert.Equal(t, arrow.Timestamp(val), arrowVal)
		assert.True(t, tvalues[idx].Equal(time.Unix(int64(arrowVal)/1000, 0)))
		assert.Equal(t, tvalues[idx], time.Unix(int64(arrowVal)/1000, 0))
	}
}

func TestDateArrow(t *testing.T) {
	meta := createMetaField(common.DataMode_REQUIRED, common.MinorType_DATE.Enum(), 1, 8)
	bindata := make([]byte, 8)
	binary.LittleEndian.PutUint64(bindata, 1203724800000)

	arr := data.NewArrowArray(bindata, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.Date64, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(arrow.Date64(0)), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, 1, arr.Len())

	date, _ := time.ParseInLocation(time.RFC3339, "2008-02-23T00:00:00+00:00", time.UTC)
	assert.True(t, date.Equal(time.Unix(int64(arr.(*array.Date64).Value(0)/1000), 0)))
}

func TestTimeArrow(t *testing.T) {
	meta := createMetaField(common.DataMode_REQUIRED, common.MinorType_TIME.Enum(), 1, 4)
	bindata := make([]byte, 4)
	binary.LittleEndian.PutUint32(bindata, 44614000)

	arr := data.NewArrowArray(bindata, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.Time32ms, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(arrow.Time32(0)), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, 1, arr.Len())

	exptime, _ := time.ParseInLocation("15:04:05 MST", "12:23:34 UTC", time.UTC)
	h, m, s := time.Unix(int64(arr.(*array.Time32).Value(0))/1000, 0).UTC().Clock()
	assert.True(t, exptime.Equal(time.Date(0, 1, 1, h, m, s, 0, time.UTC)))
}

func TestIntervalYearArrow(t *testing.T) {
	rawbin := []byte{0xf4, 0xff, 0xff, 0xff}

	meta := createMetaField(common.DataMode_REQUIRED, common.MinorType_INTERVALYEAR.Enum(), 1, 4)
	arr := data.NewArrowArray(rawbin, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.MonthInterval, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(arrow.MonthInterval(0)), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, 1, arr.Len())

	assert.Equal(t, arrow.MonthInterval(-12), arr.(*array.MonthInterval).Value(0))
}

func TestIntervalDayArrow(t *testing.T) {
	bindata := []byte{0x02, 0x00, 0x00, 0x00, 0x2b, 0x16, 0x38, 0x02, 0x11, 0x00, 0x00, 0x00}
	meta := createMetaField(common.DataMode_REQUIRED, common.MinorType_INTERVALDAY.Enum(), 1, 8)

	arr := data.NewArrowArray(bindata, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.DayTimeInterval, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(arrow.DayTimeInterval{}), data.ArrowTypeToReflect(arr.DataType()))
	assert.Equal(t, 1, arr.Len())

	dt := arr.(*array.DayTimeInterval).Value(0)
	assert.EqualValues(t, 2, dt.Days)
	dur := time.Duration(dt.Milliseconds) * time.Millisecond
	assert.Equal(t, "10h20m30.123s", dur.String())

	bindata = []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xD5, 0xE9, 0xC7, 0xFD, 0x11, 0x00, 0x00, 0x00}
	arr = data.NewArrowArray(bindata, meta)
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.FixedWidthTypes.DayTimeInterval, arr.DataType())
	assert.Equal(t, 1, arr.Len())

	dt = arr.(*array.DayTimeInterval).Value(0)
	assert.EqualValues(t, -2, dt.Days)
	dur = time.Duration(dt.Milliseconds) * time.Millisecond
	assert.Equal(t, "-10h20m30.123s", dur.String())
}

func TestNullArrowVec(t *testing.T) {
	arr := data.NewArrowArray(nil, &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_LATE.Enum(),
		}})
	assert.NotNil(t, arr)
	assert.IsType(t, arrow.Null, arr.DataType())
	assert.Exactly(t, reflect.TypeOf(nil), data.ArrowTypeToReflect(arr.DataType()))
}
