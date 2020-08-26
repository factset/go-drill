package data

import (
	"encoding/binary"
	"math"
	"math/big"
	"reflect"
	"unsafe"

	"github.com/zeroshade/go-drill/internal/rpc/proto/common"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"google.golang.org/protobuf/proto"
)

//go:generate go run ../cmd/tmpl -data numeric.tmpldata vector_numeric.gen.go.tmpl type_traits_numeric.gen.go.tmpl numeric_vec_typemap.gen.go.tmpl
//go:generate go run ../cmd/tmpl -data numeric.tmpldata type_traits_numeric.gen_test.go.tmpl vector_numeric.gen_test.go.tmpl numeric_vec_typemap.gen_test.go.tmpl

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
	val := nb.Get(index)
	if val != nil {
		return *val
	}
	return val
}

func NewNullableBitVector(data []byte, meta *shared.SerializedField) *NullableBitVector {
	bytemap := data[:meta.GetValueCount()]
	remaining := data[meta.GetValueCount():]

	return &NullableBitVector{
		NewBitVector(remaining, meta),
		bytemap,
	}
}

type VarbinaryVector struct {
	offsets []uint32
	data    []byte

	meta *shared.SerializedField
}

func (VarbinaryVector) Type() reflect.Type {
	return reflect.TypeOf([]byte{})
}

func (VarbinaryVector) TypeLen() (int64, bool) {
	return math.MaxInt64, true
}

func (v *VarbinaryVector) Len() int {
	return int(v.meta.GetValueCount())
}

func (v *VarbinaryVector) Get(index uint) []byte {
	return v.data[v.offsets[index]:v.offsets[index+1]]
}

func (v *VarbinaryVector) Value(index uint) interface{} {
	return v.Get(index)
}

func NewVarbinaryVector(data []byte, meta *shared.SerializedField) *VarbinaryVector {
	if data == nil {
		return &VarbinaryVector{
			offsets: []uint32{},
			data:    []byte{},
			meta:    meta,
		}
	}

	var offsetField *shared.SerializedField
	if meta.MajorType.GetMode() == common.DataMode_REQUIRED {
		offsetField = meta.Child[0]
	} else {
		offsetField = meta.Child[1].Child[0]
	}

	offsetBytesSize := offsetField.GetBufferLength()
	offsetBytes := data[:offsetBytesSize]
	remaining := data[offsetBytesSize:]

	offsetList := make([]uint32, meta.GetValueCount()+1)
	for i := 0; i < len(offsetList); i++ {
		offsetList[i] = binary.LittleEndian.Uint32(offsetBytes[i*4:])
	}

	return &VarbinaryVector{
		offsets: offsetList,
		data:    remaining,
		meta:    meta,
	}
}

type VarcharVector struct {
	*VarbinaryVector
}

func (VarcharVector) Type() reflect.Type {
	return reflect.TypeOf(string(""))
}

func (v *VarcharVector) Get(index uint) string {
	b := v.VarbinaryVector.Get(index)
	return *(*string)(unsafe.Pointer(&b))
}

func NewVarcharVector(data []byte, meta *shared.SerializedField) *VarcharVector {
	return &VarcharVector{NewVarbinaryVector(data, meta)}
}

type NullableVarcharVector struct {
	*VarcharVector

	byteMap []byte
}

func (nv *NullableVarcharVector) IsNull(index uint) bool {
	return nv.byteMap[index] == 0
}

func (nv *NullableVarcharVector) Get(index uint) *string {
	if nv.IsNull(index) {
		return nil
	}

	b := nv.VarbinaryVector.Get(index)
	return (*string)(unsafe.Pointer(&b))
}

func (nv *NullableVarcharVector) Value(index uint) interface{} {
	val := nv.Get(index)
	if val == nil {
		return nil
	}

	return *val
}

func NewNullableVarcharVector(data []byte, meta *shared.SerializedField) *NullableVarcharVector {
	byteMap := data[:meta.GetValueCount()]
	remaining := data[meta.GetValueCount():]

	return &NullableVarcharVector{
		NewVarcharVector(remaining, meta),
		byteMap,
	}
}

type DecimalVector struct {
	*fixedWidthVec

	traits DecimalTraits
	scale  int
	prec   int32
}

func NewDecimalVector(data []byte, meta *shared.SerializedField, traits DecimalTraits) *DecimalVector {
	return &DecimalVector{
		fixedWidthVec: &fixedWidthVec{data: data, valsz: traits.ByteWidth(), meta: meta},
		scale:         int(meta.MajorType.GetScale()),
		prec:          meta.MajorType.GetPrecision(),
		traits:        traits,
	}
}

func (dv *DecimalVector) Get(index uint) *big.Float {
	valbytes := dv.getval(int(index))

	return getFloatFromBytes(valbytes, dv.traits.NumDigits(), dv.scale, dv.traits.IsSparse())
}

func (dv *DecimalVector) Value(index uint) interface{} {
	return dv.Get(index)
}

type NullableDecimalVector struct {
	*nullableFixedWidthVec

	traits DecimalTraits
	scale  int
	prec   int32
}

func (dv *NullableDecimalVector) Get(index uint) *big.Float {
	valbytes := dv.getval(int(index))
	if valbytes == nil {
		return nil
	}

	return getFloatFromBytes(valbytes, dv.traits.NumDigits(), dv.scale, dv.traits.IsSparse())
}

func (dv *NullableDecimalVector) Value(index uint) interface{} {
	return dv.Get(index)
}

func NewNullableDecimalVector(data []byte, meta *shared.SerializedField, traits DecimalTraits) *NullableDecimalVector {
	return &NullableDecimalVector{
		nullableFixedWidthVec: newNullableFixedWidth(data, meta, traits.ByteWidth()),
		scale:                 int(meta.MajorType.GetScale()),
		prec:                  meta.MajorType.GetPrecision(),
		traits:                traits,
	}
}

func NewValueVec(rawData []byte, meta *shared.SerializedField) DataVector {
	ret := NewNumericValueVec(rawData, meta)
	if ret != nil {
		return ret
	}

	if meta.GetMajorType().GetMode() == common.DataMode_OPTIONAL {
		switch meta.GetMajorType().GetMinorType() {
		case common.MinorType_BIT:
			return NewNullableBitVector(rawData, meta)
		case common.MinorType_VARCHAR:
			return NewNullableVarcharVector(rawData, meta)
		case common.MinorType_TIMESTAMP:
			return NewNullableTimestampVector(rawData, meta)
		case common.MinorType_DATE:
			return NewNullableDateVector(rawData, meta)
		case common.MinorType_TIME:
			return NewNullableTimeVector(rawData, meta)
		case common.MinorType_INTERVAL:
			return NewNullableIntervalVector(rawData, meta)
		case common.MinorType_INTERVALDAY:
			return NewNullableIntervalDayVector(rawData, meta)
		case common.MinorType_INTERVALYEAR:
			return NewNullableIntervalYearVector(rawData, meta)
		case common.MinorType_DECIMAL28SPARSE:
			return NewNullableDecimalVector(rawData, meta, &Decimal28SparseTraits)
		case common.MinorType_DECIMAL38SPARSE:
			return NewNullableDecimalVector(rawData, meta, &Decimal38SparseTraits)
		}
	} else {
		switch meta.GetMajorType().GetMinorType() {
		case common.MinorType_VARBINARY:
			return NewVarbinaryVector(rawData, meta)
		case common.MinorType_VARCHAR:
			return NewVarcharVector(rawData, meta)
		case common.MinorType_BIT:
			return NewBitVector(rawData, meta)
		case common.MinorType_TIMESTAMP:
			return NewTimestampVector(rawData, meta)
		case common.MinorType_DATE:
			return NewDateVector(rawData, meta)
		case common.MinorType_TIME:
			return NewTimeVector(rawData, meta)
		case common.MinorType_INTERVAL:
			return NewIntervalVector(rawData, meta)
		case common.MinorType_INTERVALDAY:
			return NewIntervalDayVector(rawData, meta)
		case common.MinorType_INTERVALYEAR:
			return NewIntervalYearVector(rawData, meta)
		case common.MinorType_DECIMAL28SPARSE:
			return NewDecimalVector(rawData, meta, &Decimal28SparseTraits)
		case common.MinorType_DECIMAL38SPARSE:
			return NewDecimalVector(rawData, meta, &Decimal38SparseTraits)
		}
	}

	return nil
}
