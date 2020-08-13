package data

import (
	"encoding/binary"
	"math"
	"reflect"
	"time"
	"unsafe"

	"github.com/zeroshade/go-drill/internal/rpc/proto/common"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"google.golang.org/protobuf/proto"
)

type DataVector interface {
	Len() int
	Value(index uint) interface{}
	Type() reflect.Type
	TypeLen() (int64, bool)
}

type NullableDataVector interface {
	DataVector
	IsNull(index uint) bool
}

type BitVector struct {
	values []byte
	meta   *shared.SerializedField
}

func (BitVector) Type() reflect.Type {
	return reflect.TypeOf(bool(false))
}

func (BitVector) TypeLen() (int64, bool) {
	return 0, false
}

func (b *BitVector) Len() int {
	return int(b.meta.GetValueCount())
}

func (b *BitVector) Get(index uint) bool {
	bt := b.values[index/8]
	return bt&(1<<(index%8)) != 0
}

func (b *BitVector) Value(index uint) interface{} {
	return b.Get(index)
}

func NewBitVector(data []byte, meta *shared.SerializedField) *BitVector {
	return &BitVector{
		values: data,
		meta:   meta,
	}
}

type NullableBitVector struct {
	*BitVector

	byteMap []byte
}

func (nb *NullableBitVector) IsNull(index uint) bool {
	return nb.byteMap[index] == 0
}

func (nb *NullableBitVector) Get(index uint) *bool {
	if nb.IsNull(index) {
		return nil
	}

	return proto.Bool(nb.BitVector.Get(index))
}

func (nb *NullableBitVector) Value(index uint) interface{} {
	return nb.Get(index)
}

type VarcharVector struct {
	offsets []uint32
	data    []byte

	meta *shared.SerializedField
}

func (VarcharVector) Type() reflect.Type {
	return reflect.TypeOf(string(""))
}

func (VarcharVector) TypeLen() (int64, bool) {
	return math.MaxInt64, true
}

func (v *VarcharVector) Len() int {
	return int(v.meta.GetValueCount())
}

func (v *VarcharVector) Get(index uint) []byte {
	return v.data[v.offsets[index]:v.offsets[index+1]]
}

func (v *VarcharVector) GetString(index uint) string {
	b := v.Get(index)
	return *(*string)(unsafe.Pointer(&b))
}

func (v *VarcharVector) Value(index uint) interface{} {
	return v.Get(index)
}

func NewVarcharVector(data []byte, meta *shared.SerializedField) *VarcharVector {
	offsetBytesSize := meta.Child[1].Child[0].GetBufferLength()
	offsetBytes := data[:offsetBytesSize]
	remaining := data[offsetBytesSize:]

	offsetList := make([]uint32, meta.GetValueCount()+1)
	for i := 0; i < len(offsetList); i++ {
		offsetList[i] = binary.LittleEndian.Uint32(offsetBytes[i*4:])
	}

	return &VarcharVector{
		offsets: offsetList,
		data:    remaining,
		meta:    meta,
	}
}

type NullableVarcharVector struct {
	*VarcharVector

	byteMap []byte
}

func (nv *NullableVarcharVector) IsNull(index uint) bool {
	return nv.byteMap[index] == 0
}

func (nv *NullableVarcharVector) Get(index uint) []byte {
	if nv.IsNull(index) {
		return nil
	}

	return nv.VarcharVector.Get(index)
}

func (nv *NullableVarcharVector) GetString(index uint) *string {
	b := nv.Get(index)
	return (*string)(unsafe.Pointer(&b))
}

func (nv *NullableVarcharVector) Value(index uint) interface{} {
	return nv.Get(index)
}

func NewNullableVarcharVector(data []byte, meta *shared.SerializedField) *NullableVarcharVector {
	byteMap := data[:meta.GetValueCount()]
	remaining := data[meta.GetValueCount():]

	return &NullableVarcharVector{
		NewVarcharVector(remaining, meta),
		byteMap,
	}
}

type TimestampVector struct {
	*Int64Vector
}

func NewTimestampVector(data []byte, meta *shared.SerializedField) *TimestampVector {
	return &TimestampVector{
		NewInt64Vector(data, meta),
	}
}

func (v *TimestampVector) Get(index uint) time.Time {
	ts := v.Int64Vector.Get(index)
	return time.Unix(ts, 0)
}

func (v *TimestampVector) Value(index uint) interface{} {
	return v.Get(index)
}

type NullableTimestampVector struct {
	*NullableInt64Vector
}

func (v *NullableTimestampVector) Get(index uint) time.Time {
	ts := v.NullableInt64Vector.Get(index)
	if ts == nil {
		return time.Time{}
	}

	return time.Unix(*ts/1000, 0)
}

func (v *NullableTimestampVector) Value(index uint) interface{} {
	return v.Get(index)
}

func NewNullableTimestampVector(data []byte, meta *shared.SerializedField) *NullableTimestampVector {
	return &NullableTimestampVector{
		NewNullableInt64Vector(data, meta),
	}
}

func NewValueVec(rawData []byte, meta *shared.SerializedField) DataVector {
	ret := NewNumericValueVec(rawData, meta)
	if ret != nil {
		return ret
	}

	if meta.GetMajorType().GetMode() == common.DataMode_OPTIONAL {
		switch meta.GetMajorType().GetMinorType() {
		case common.MinorType_VARCHAR:
			return NewNullableVarcharVector(rawData, meta)
		case common.MinorType_TIMESTAMP:
			return NewNullableTimestampVector(rawData, meta)
		}
	}

	switch meta.GetMajorType().GetMinorType() {
	case common.MinorType_BIT:
		return NewBitVector(rawData, meta)
	case common.MinorType_TIMESTAMP:
		return NewTimestampVector(rawData, meta)
	}

	return nil
}
