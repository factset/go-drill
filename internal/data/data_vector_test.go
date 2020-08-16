package data_test

import (
	"encoding/binary"
	"math"
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
	meta := &shared.SerializedField{ValueCount: proto.Int32(8)}
	bytemap := []byte{0, 1, 0, 1, 0, 1, 0, 1}
	// bit pattern of 0xaa is 10101010
	vec := data.NewNullableBitVector(append(bytemap, []byte{0xaa}...), meta)

	assert.Exactly(t, reflect.TypeOf(bool(false)), vec.Type())
	assert.Equal(t, 8, vec.Len())

	l, ok := vec.TypeLen()
	assert.Zero(t, l)
	assert.False(t, ok)

	for i := 0; i < 8; i++ {
		assert.Equal(t, i%2 == 0, vec.IsNull(uint(i)))
		if i%2 == 0 {
			assert.Nil(t, vec.Get(uint(i)))
			assert.Nil(t, vec.Value(uint(i)))
		} else {
			assert.Exactly(t, proto.Bool(true), vec.Get(uint(i)))
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
	assert.Exactly(t, reflect.TypeOf(int64(0)), vec.Type())

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
	assert.Exactly(t, reflect.TypeOf(int64(0)), vec.Type())

	l, ok := vec.TypeLen()
	assert.Zero(t, l)
	assert.False(t, ok)

	for idx, val := range values {
		stamp := time.Unix(val/1000, val%1000)

		if idx%2 == 1 {
			assert.True(t, vec.Get(uint(idx)).IsZero())
			assert.True(t, vec.Value(uint(idx)).(time.Time).IsZero())
		} else {
			assert.Exactly(t, stamp, vec.Get(uint(idx)))
			assert.Exactly(t, stamp, vec.Value(uint(idx)))
		}
	}
}

func TestValueVecNil(t *testing.T) {
	assert.Nil(t, data.NewValueVec(nil, &shared.SerializedField{
		MajorType: &common.MajorType{
			MinorType: common.MinorType_LATE.Enum(),
		},
	}))
}
