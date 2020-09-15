package data_test

import (
	"encoding/binary"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/common"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"google.golang.org/protobuf/proto"
)

func TestBitVector(t *testing.T) {
	meta := &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_BIT.Enum(),
			Mode:      common.DataMode_REQUIRED.Enum(),
		},
		ValueCount: proto.Int32(8),
	}
	// bit pattern of 0xaa is 10101010
	dv := data.NewValueVec([]byte{0xaa}, meta)

	vec := dv.(*data.BitVector)
	assert.Exactly(t, reflect.TypeOf(bool(false)), vec.Type())
	assert.Equal(t, 8, vec.Len())

	l, ok := vec.TypeLen()
	assert.Zero(t, l)
	assert.False(t, ok)

	for i := 0; i < 8; i++ {
		assert.Exactly(t, i%2 == 1, vec.Get(uint(i)))
		assert.Exactly(t, i%2 == 1, vec.Value(uint(i)))
	}
}

func TestNullableBitVector(t *testing.T) {
	meta := &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_BIT.Enum(),
		},
		ValueCount: proto.Int32(8),
	}
	bytemap := []byte{0, 1, 0, 1, 0, 1, 0, 1}
	// bit pattern of 0xaa is 10101010
	vec := data.NewValueVec(append(bytemap, []byte{0xaa}...), meta)

	assert.Exactly(t, reflect.TypeOf(bool(false)), vec.Type())
	assert.Equal(t, 8, vec.Len())

	l, ok := vec.TypeLen()
	assert.Zero(t, l)
	assert.False(t, ok)

	for i := 0; i < 8; i++ {
		assert.Equal(t, i%2 == 0, vec.(data.NullableDataVector).IsNull(uint(i)))
		if i%2 == 0 {
			assert.Nil(t, vec.(*data.NullableBitVector).Get(uint(i)))
			assert.Nil(t, vec.Value(uint(i)))
		} else {
			assert.Exactly(t, proto.Bool(true), vec.(*data.NullableBitVector).Get(uint(i)))
			assert.Exactly(t, true, vec.Value(uint(i)))
		}
	}
}

var varchardata = []string{"foo", "bar", "edelgard", "logan", "penny", "superbad", "howdy"}

func getVarcharBytes() ([]byte, *shared.SerializedField) {
	offsets := make([]byte, (len(varchardata)+1)*4)
	data := make([]byte, 0)

	curOffset := uint32(0)
	for idx, s := range varchardata {
		binary.LittleEndian.PutUint32(offsets[idx*4:], curOffset)
		curOffset += uint32(len(s))

		data = append(data, []byte(s)...)
	}

	binary.LittleEndian.PutUint32(offsets[len(offsets)-4:], curOffset)

	meta := &shared.SerializedField{
		ValueCount:   proto.Int32(int32(len(varchardata))),
		BufferLength: proto.Int32(int32(len(offsets) + len(data))),
		Child: []*shared.SerializedField{
			{
				NamePart:     &shared.NamePart{Name: proto.String("$offsets$")},
				ValueCount:   proto.Int32(int32(len(varchardata)) + 1),
				BufferLength: proto.Int32(int32(len(offsets))),
			},
		},
	}

	return append(offsets, data...), meta
}

func TestVarbinaryVector(t *testing.T) {
	vbytes, meta := getVarcharBytes()

	meta.MajorType = &common.MajorType{
		MinorType: common.MinorType_VARBINARY.Enum(),
		Mode:      common.DataMode_REQUIRED.Enum(),
	}
	dv := data.NewValueVec(vbytes, meta)
	vec := dv.(*data.VarbinaryVector)

	assert.Exactly(t, reflect.TypeOf([]byte{}), vec.Type())
	l, ok := vec.TypeLen()
	assert.Exactly(t, int64(math.MaxInt64), l)
	assert.True(t, ok)

	assert.Equal(t, len(varchardata), vec.Len())
	for idx, val := range varchardata {
		assert.Equal(t, []byte(val), vec.Get(uint(idx)))
		assert.Equal(t, []byte(val), vec.Value(uint(idx)))
	}
}

func TestVarcharVector(t *testing.T) {
	vbytes, meta := getVarcharBytes()

	meta.MajorType = &common.MajorType{
		MinorType: common.MinorType_VARCHAR.Enum(),
		Mode:      common.DataMode_REQUIRED.Enum(),
	}
	dv := data.NewValueVec(vbytes, meta)
	vec := dv.(*data.VarcharVector)

	assert.Exactly(t, reflect.TypeOf(string("")), vec.Type())
	l, ok := vec.TypeLen()
	assert.Exactly(t, int64(math.MaxInt64), l)
	assert.True(t, ok)

	assert.Equal(t, len(varchardata), vec.Len())
	for idx, val := range varchardata {
		assert.Equal(t, val, vec.Get(uint(idx)))
		assert.Equal(t, []byte(val), vec.Value(uint(idx)))
	}
}

func TestNullableVarcharVector(t *testing.T) {
	vbytes, meta := getVarcharBytes()

	bytemap := make([]byte, len(varchardata))
	for idx := range varchardata {
		bytemap[idx] = byte(idx % 2)
	}

	dv := data.NewValueVec(append(bytemap, vbytes...), &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_VARCHAR.Enum(),
			Mode:      common.DataMode_OPTIONAL.Enum(),
		},
		ValueCount: proto.Int32(int32(len(varchardata))),
		Child:      []*shared.SerializedField{{}, meta},
	})
	vec := dv.(*data.NullableVarcharVector)

	assert.Exactly(t, reflect.TypeOf(string("")), vec.Type())
	l, ok := vec.TypeLen()
	assert.Exactly(t, int64(math.MaxInt64), l)
	assert.True(t, ok)

	assert.Equal(t, len(varchardata), vec.Len())
	for idx, val := range varchardata {
		assert.Exactly(t, idx%2 == 0, vec.IsNull(uint(idx)))

		if idx%2 == 0 {
			assert.Nil(t, vec.Get(uint(idx)))
			assert.Nil(t, vec.Value(uint(idx)))
		} else {
			assert.Equal(t, proto.String(val), vec.Get(uint(idx)))
			assert.Equal(t, val, vec.Value(uint(idx)))
		}
	}
}

func TestNilVarbinaryVector(t *testing.T) {
	vec := data.NewVarbinaryVector(nil, &shared.SerializedField{ValueCount: proto.Int32(0)})
	assert.Zero(t, vec.Len())
}

func TestTimestampVector(t *testing.T) {
	const N = 10
	values := make([]int64, N)
	for idx := range values {
		values[idx] = time.Now().Add(time.Duration(-1*idx) * time.Hour).Unix()
	}

	b := data.Int64Traits.CastToBytes(values)
	dv := data.NewValueVec(b, &shared.SerializedField{
		MajorType: &common.MajorType{
			Mode:      common.DataMode_REQUIRED.Enum(),
			MinorType: common.MinorType_TIMESTAMP.Enum(),
		},
	})
	vec := dv.(*data.TimestampVector)

	assert.Equal(t, N, vec.Len())
	assert.Exactly(t, reflect.TypeOf(time.Time{}), vec.Type())

	l, ok := vec.TypeLen()
	assert.Zero(t, l)
	assert.False(t, ok)

	for idx, val := range values {
		stamp := time.Unix(val/1000, val%1000)

		assert.Exactly(t, stamp, vec.Get(uint(idx)))
		assert.Exactly(t, stamp, vec.Value(uint(idx)))
	}
}

func TestNullableTimestampVector(t *testing.T) {
	const N = 10
	values := make([]int64, N)
	for idx := range values {
		values[idx] = time.Now().Add(time.Duration(-1*idx) * time.Hour).Unix()
	}

	b := data.Int64Traits.CastToBytes(values)

	bytemap := []byte{1, 0, 1, 0, 1, 0, 1, 0, 1, 0}
	dv := data.NewValueVec(append(bytemap, b...), &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_TIMESTAMP.Enum(),
		},
		ValueCount: proto.Int32(N),
	})
	vec := dv.(*data.NullableTimestampVector)

	assert.Equal(t, N, vec.Len())
	assert.Exactly(t, reflect.TypeOf(time.Time{}), vec.Type())

	l, ok := vec.TypeLen()
	assert.Zero(t, l)
	assert.False(t, ok)

	for idx, val := range values {
		stamp := time.Unix(val/1000, val%1000)

		if idx%2 == 1 {
			assert.Nil(t, vec.Get(uint(idx)))
			assert.Nil(t, vec.Value(uint(idx)))
		} else {
			assert.Exactly(t, &stamp, vec.Get(uint(idx)))
			assert.Exactly(t, stamp, vec.Value(uint(idx)))
		}
	}
}

func TestDateVector(t *testing.T) {
	meta := &shared.SerializedField{
		ValueCount:   proto.Int32(1),
		BufferLength: proto.Int32(8),
		MajorType: &common.MajorType{
			MinorType: common.MinorType_DATE.Enum(), Mode: common.DataMode_REQUIRED.Enum(),
		},
		NamePart: &shared.NamePart{Name: proto.String("EXPR$0")},
	}

	bindata := make([]byte, 8)
	binary.LittleEndian.PutUint64(bindata, 1203724800000)

	vec := data.NewValueVec(bindata, meta)
	date, _ := time.ParseInLocation(time.RFC3339, "2008-02-23T00:00:00+00:00", time.UTC)

	assert.Equal(t, date, vec.Value(0))

	bytemap := []byte{0}
	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, bindata...), meta)
	assert.Nil(t, vec.Value(0))

	bytemap = []byte{1}
	vec = data.NewValueVec(append(bytemap, bindata...), meta)
	assert.Equal(t, date, vec.Value(0))
	assert.Exactly(t, time.UTC, vec.Value(0).(time.Time).Location())
}

func TestTimeVector(t *testing.T) {
	meta := &shared.SerializedField{
		ValueCount:   proto.Int32(1),
		BufferLength: proto.Int32(4),
		MajorType: &common.MajorType{
			MinorType: common.MinorType_TIME.Enum(), Mode: common.DataMode_REQUIRED.Enum(),
		},
		NamePart: &shared.NamePart{Name: proto.String("EXPR$0")},
	}

	bindata := make([]byte, 4)
	binary.LittleEndian.PutUint32(bindata, 44614000)

	vec := data.NewValueVec(bindata, meta)
	assert.Exactly(t, reflect.TypeOf(time.Time{}), vec.Type())
	exptime, _ := time.ParseInLocation("15:04:05 MST", "12:23:34 UTC", time.UTC)
	assert.Equal(t, exptime, vec.Value(0))

	bytemap := []byte{0}
	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, bindata...), meta)
	assert.Exactly(t, reflect.TypeOf(time.Time{}), vec.Type())
	assert.Nil(t, vec.Value(0))

	bytemap = []byte{1}
	vec = data.NewValueVec(append(bytemap, bindata...), meta)
	assert.Equal(t, exptime, vec.Value(0))
}

func TestIntervalYearVector(t *testing.T) {
	rawbin := []byte{0xf4, 0xff, 0xff, 0xff}

	meta := &shared.SerializedField{
		ValueCount:   proto.Int32(1),
		BufferLength: proto.Int32(4),
		MajorType: &common.MajorType{
			MinorType: common.MinorType_INTERVALYEAR.Enum(), Mode: common.DataMode_REQUIRED.Enum(),
		},
		NamePart: &shared.NamePart{Name: proto.String("EXPR$0")},
	}

	vec := data.NewValueVec(rawbin, meta)
	assert.EqualValues(t, "-1-0", vec.Value(0))

	bytemap := []byte{0}
	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, rawbin...), meta)
	assert.Nil(t, vec.Value(0))
}

func TestIntervalDayVector(t *testing.T) {
	bindata := []byte{0x02, 0x00, 0x00, 0x00, 0x2b, 0x16, 0x38, 0x02, 0x11, 0x00, 0x00, 0x00}

	meta := &shared.SerializedField{
		ValueCount:   proto.Int32(1),
		BufferLength: proto.Int32(8),
		MajorType: &common.MajorType{
			MinorType: common.MinorType_INTERVALDAY.Enum(), Mode: common.DataMode_REQUIRED.Enum(),
		},
		NamePart: &shared.NamePart{Name: proto.String("EXPR$0")},
	}

	vec := data.NewValueVec(bindata, meta)
	assert.EqualValues(t, "2 days 10h20m30.123s", vec.Value(0))

	bindata = []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xD5, 0xE9, 0xC7, 0xFD, 0x11, 0x00, 0x00, 0x00}
	vec = data.NewValueVec(bindata, meta)
	assert.EqualValues(t, "-2 days 10h20m30.123s", vec.Value(0))

	bytemap := []byte{0}
	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, bindata...), meta)
	assert.Nil(t, vec.Value(0))
}

func TestIntervalFull(t *testing.T) {
	bindata := make([]byte, 12)
	binary.LittleEndian.PutUint32(bindata, 13)
	binary.LittleEndian.PutUint32(bindata[4:], 120)
	binary.LittleEndian.PutUint32(bindata[8:], uint32(time.Duration(10*time.Hour+20*time.Minute+30*time.Second).Milliseconds()))

	meta := &shared.SerializedField{
		ValueCount:   proto.Int32(1),
		BufferLength: proto.Int32(12),
		MajorType: &common.MajorType{
			MinorType: common.MinorType_INTERVAL.Enum(), Mode: common.DataMode_REQUIRED.Enum(),
		},
		NamePart: &shared.NamePart{Name: proto.String("EXPR$0")},
	}

	vec := data.NewValueVec(bindata, meta)
	assert.EqualValues(t, "1-1-120 10h20m30s", vec.Value(0))

	assert.EqualValues(t, reflect.TypeOf(string("")), vec.Type())
	l, ok := vec.TypeLen()
	assert.Zero(t, l)
	assert.False(t, ok)

	assert.EqualValues(t, 1, vec.Len())

	bytemap := []byte{1}
	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, bindata...), meta)
	assert.EqualValues(t, "1-1-120 10h20m30s", vec.Value(0))

	v := int32(-13)
	binary.LittleEndian.PutUint32(bindata, uint32(v))
	v = int32(-10)
	binary.LittleEndian.PutUint32(bindata[4:], uint32(v))
	v = int32(time.Duration(10*time.Hour + 20*time.Minute + 30*time.Second).Milliseconds())
	binary.LittleEndian.PutUint32(bindata[8:], uint32(v))

	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, bindata...), meta)
	assert.EqualValues(t, "-1-1-10 10h20m30s", vec.Value(0))
}

func TestValueVecNil(t *testing.T) {
	assert.Nil(t, data.NewValueVec(nil, &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_LATE.Enum(),
		},
	}))
}

var d38sparse1 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x50, 0x76, 0x33, 0x0B}
var d38sparse2 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF8, 0x03, 0x68, 0x0F}
var d38sparse3 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x18, 0x77, 0x2F, 0x14}
var d38sparse4 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0xD8, 0x4E, 0xDA, 0x17}
var d38sparse5 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0xF8, 0x5B, 0x11, 0x22}

var d38sparseResults = []string{"0.187922", "0.258475", "0.338655", "1.400183", "2.571563"}

func TestDecimal38Sparse(t *testing.T) {
	buffer := append(d38sparse1, d38sparse2...)
	buffer = append(buffer, d38sparse3...)
	buffer = append(buffer, d38sparse4...)
	buffer = append(buffer, d38sparse5...)

	meta := &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_DECIMAL38SPARSE.Enum(),
			Mode:      common.DataMode_REQUIRED.Enum(),
			Scale:     proto.Int32(6),
			Precision: proto.Int32(20),
		},
		BufferLength: proto.Int32(120),
		ValueCount:   proto.Int32(5),
	}

	vec := data.NewValueVec(buffer, meta)
	for i := 0; i < vec.Len(); i++ {
		val := vec.Value(uint(i))
		assert.NotNil(t, val)

		assert.IsType(t, (*big.Float)(nil), val)
		assert.EqualValues(t, d38sparseResults[i], val.(*big.Float).String())
	}

	bytemap := []byte{1, 0, 1, 0, 1}
	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, buffer...), meta)
	for i := 0; i < vec.Len(); i++ {
		val := vec.Value(uint(i))

		if i%2 == 0 {
			assert.NotNil(t, val)
			assert.IsType(t, (*big.Float)(nil), val)
			assert.EqualValues(t, d38sparseResults[i], val.(*big.Float).String())
		} else {
			assert.Nil(t, val)
		}
	}
}

// adding the 128 byte makes some of them negative to test with
var d28sparse1 = []byte{0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x50, 0x76, 0x33, 0x0B}
var d28sparse2 = []byte{0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF8, 0x03, 0x68, 0x0F}
var d28sparse3 = []byte{0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x18, 0x77, 0x2F, 0x14}
var d28sparse4 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0xD8, 0x4E, 0xDA, 0x17}
var d28sparse5 = []byte{0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0xF8, 0x5B, 0x11, 0x22}

var d28sparseResults = []string{"-0.187922", "-0.258475", "-0.338655", "1.400183", "-2.571563"}

func TestDecimal28Sparse(t *testing.T) {
	buffer := append(d28sparse1, d28sparse2...)
	buffer = append(buffer, d28sparse3...)
	buffer = append(buffer, d28sparse4...)
	buffer = append(buffer, d28sparse5...)

	meta := &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_DECIMAL28SPARSE.Enum(),
			Mode:      common.DataMode_REQUIRED.Enum(),
			Scale:     proto.Int32(5),
			Precision: proto.Int32(20),
		},
		BufferLength: proto.Int32(100),
		ValueCount:   proto.Int32(5),
	}

	vec := data.NewValueVec(buffer, meta)
	for i := 0; i < vec.Len(); i++ {
		val := vec.Value(uint(i))
		assert.NotNil(t, val)

		assert.IsType(t, (*big.Float)(nil), val)
		assert.EqualValues(t, d28sparseResults[i], val.(*big.Float).String())
	}

	bytemap := []byte{1, 0, 1, 0, 1}
	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	vec = data.NewValueVec(append(bytemap, buffer...), meta)
	for i := 0; i < vec.Len(); i++ {
		val := vec.Value(uint(i))

		if i%2 == 0 {
			assert.NotNil(t, val)
			assert.IsType(t, (*big.Float)(nil), val)
			assert.EqualValues(t, d28sparseResults[i], val.(*big.Float).String())
		} else {
			assert.Nil(t, val)
		}
	}
}

func TestDecimalDense(t *testing.T) {
	buffer := append(d38sparse1, d38sparse2...)
	buffer = append(buffer, d38sparse3...)
	buffer = append(buffer, d38sparse4...)
	buffer = append(buffer, d38sparse5...)

	meta := &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_DECIMAL38DENSE.Enum(),
			Mode:      common.DataMode_REQUIRED.Enum(),
			Scale:     proto.Int32(6),
			Precision: proto.Int32(20),
		},
		BufferLength: proto.Int32(120),
		ValueCount:   proto.Int32(5),
	}

	vec := data.NewValueVec(buffer, meta)
	assert.Nil(t, vec)

	vec = data.NewDecimalVector(buffer, meta, data.Decimal38DenseTraits)
	assert.PanicsWithValue(t, "go-drill: currently only supports decimal sparse vectors, not dense", func() {
		vec.Value(uint(0))
	})

	meta.MajorType.MinorType = common.MinorType_DECIMAL28DENSE.Enum()
	vec = data.NewValueVec(buffer, meta)
	assert.Nil(t, vec)

	vec = data.NewDecimalVector(buffer, meta, data.Decimal28DenseTraits)
	assert.PanicsWithValue(t, "go-drill: currently only supports decimal sparse vectors, not dense", func() {
		vec.Value(uint(0))
	})

	meta.MajorType.Mode = common.DataMode_OPTIONAL.Enum()
	bytemap := []byte{1, 1, 1, 1, 1}
	vec = data.NewValueVec(append(bytemap, buffer...), meta)
	assert.Nil(t, vec)

	vec = data.NewNullableDecimalVector(append(bytemap, buffer...), meta, data.Decimal28DenseTraits)
	assert.PanicsWithValue(t, "go-drill: currently only supports decimal sparse vectors, not dense", func() {
		vec.Value(uint(0))
	})

	meta.MajorType.MinorType = common.MinorType_DECIMAL38DENSE.Enum()
	vec = data.NewValueVec(buffer, meta)
	assert.Nil(t, vec)

	vec = data.NewNullableDecimalVector(append(bytemap, buffer...), meta, data.Decimal38DenseTraits)
	assert.PanicsWithValue(t, "go-drill: currently only supports decimal sparse vectors, not dense", func() {
		vec.Value(uint(0))
	})
}
