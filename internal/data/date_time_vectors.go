package data

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
)

type TimestampVector struct {
	*Int64Vector
}

func (TimestampVector) Type() reflect.Type {
	return reflect.TypeOf(time.Time{})
}

func NewTimestampVector(data []byte, meta *shared.SerializedField) *TimestampVector {
	return &TimestampVector{
		NewInt64Vector(data, meta),
	}
}

func (v *TimestampVector) Get(index uint) time.Time {
	ts := v.Int64Vector.Get(index)
	return time.Unix(ts/1000, ts%1000)
}

func (v *TimestampVector) Value(index uint) interface{} {
	return v.Get(index)
}

type DateVector struct {
	*TimestampVector
}

func (dv *DateVector) Get(index uint) time.Time {
	return dv.TimestampVector.Get(index).UTC()
}

func (dv *DateVector) Value(index uint) interface{} {
	return dv.Get(index)
}

func NewDateVector(data []byte, meta *shared.SerializedField) *DateVector {
	return &DateVector{NewTimestampVector(data, meta)}
}

type TimeVector struct {
	*Int32Vector
}

func (TimeVector) Type() reflect.Type {
	return reflect.TypeOf(time.Time{})
}

func (t *TimeVector) Get(index uint) time.Time {
	ts := t.Int32Vector.Get(index)
	h, m, s := time.Unix(int64(ts/1000), int64(ts%1000)).UTC().Clock()
	return time.Date(0, 1, 1, h, m, s, 0, time.UTC)
}

func (t *TimeVector) Value(index uint) interface{} {
	return t.Get(index)
}

func NewTimeVector(date []byte, meta *shared.SerializedField) *TimeVector {
	return &TimeVector{NewInt32Vector(date, meta)}
}

type NullableTimestampVector struct {
	*NullableInt64Vector
}

func (NullableTimestampVector) Type() reflect.Type {
	return reflect.TypeOf(time.Time{})
}

func (v *NullableTimestampVector) Get(index uint) *time.Time {
	ts := v.NullableInt64Vector.Get(index)
	if ts == nil {
		return nil
	}

	ret := time.Unix(*ts/1000, *ts%1000)
	return &ret
}

func (v *NullableTimestampVector) Value(index uint) interface{} {
	val := v.Get(index)
	if val != nil {
		return *val
	}

	return val
}

func NewNullableTimestampVector(data []byte, meta *shared.SerializedField) *NullableTimestampVector {
	return &NullableTimestampVector{
		NewNullableInt64Vector(data, meta),
	}
}

type NullableDateVector struct {
	*NullableTimestampVector
}

func (nv *NullableDateVector) Get(index uint) *time.Time {
	ret := nv.NullableTimestampVector.Get(index)
	if ret != nil {
		*ret = ret.UTC()
	}

	return ret
}

func (nv *NullableDateVector) Value(index uint) interface{} {
	ret := nv.Get(index)
	if ret != nil {
		return *ret
	}

	return nil
}

func NewNullableDateVector(data []byte, meta *shared.SerializedField) *NullableDateVector {
	return &NullableDateVector{NewNullableTimestampVector(data, meta)}
}

type NullableTimeVector struct {
	*NullableInt32Vector
}

func (NullableTimeVector) Type() reflect.Type {
	return reflect.TypeOf(time.Time{})
}

func (v *NullableTimeVector) Get(index uint) *time.Time {
	ts := v.NullableInt32Vector.Get(index)
	if ts == nil {
		return nil
	}

	h, m, s := time.Unix(int64(*ts/1000), int64(*ts%1000)).UTC().Clock()
	ret := time.Date(0, 1, 1, h, m, s, 0, time.UTC)
	return &ret
}

func (v *NullableTimeVector) Value(index uint) interface{} {
	val := v.Get(index)
	if val != nil {
		return *val
	}

	return val
}

func NewNullableTimeVector(data []byte, meta *shared.SerializedField) *NullableTimeVector {
	return &NullableTimeVector{NewNullableInt32Vector(data, meta)}
}

type intervalBase interface {
	Type() reflect.Type
	TypeLen() (int64, bool)
	Len() int
	GetRawBytes() []byte
	getval(index int) []byte
}

type fixedWidthVec struct {
	data  []byte
	valsz int

	meta *shared.SerializedField
}

func (fixedWidthVec) Type() reflect.Type {
	return reflect.TypeOf(string(""))
}

func (fixedWidthVec) TypeLen() (int64, bool) {
	return 0, false
}

func (v *fixedWidthVec) GetRawBytes() []byte {
	return v.data
}

func (v *fixedWidthVec) Len() int {
	return int(v.meta.GetValueCount())
}

func (v *fixedWidthVec) getval(index int) []byte {
	start := index * v.valsz
	return v.data[start : start+v.valsz]
}

type nullableIntervalBase interface {
	intervalBase
	IsNull(index uint) bool
	GetNullBytemap() []byte
}

type nullableFixedWidthVec struct {
	*fixedWidthVec
	nullByteMap
}

func (nv *nullableFixedWidthVec) GetNullBytemap() []byte {
	return nv.byteMap
}

func (nv *nullableFixedWidthVec) getval(index int) []byte {
	if nv.IsNull(uint(index)) {
		return nil
	}
	return nv.fixedWidthVec.getval(index)
}

func newNullableFixedWidth(data []byte, meta *shared.SerializedField, valsz int) *nullableFixedWidthVec {
	byteMap := data[:meta.GetValueCount()]
	remaining := data[meta.GetValueCount():]

	return &nullableFixedWidthVec{
		&fixedWidthVec{remaining, valsz, meta},
		nullByteMap{byteMap},
	}
}

type intervalVector struct {
	intervalBase
	process func([]byte) string
}

func (iv *intervalVector) Get(index uint) string {
	return iv.process(iv.getval(int(index)))
}

func (iv *intervalVector) Value(index uint) interface{} {
	return iv.Get(index)
}

type nullableIntervalVector struct {
	nullableIntervalBase
	process func([]byte) string
}

func (iv *nullableIntervalVector) Get(index uint) *string {
	data := iv.getval(int(index))
	if data == nil {
		return nil
	}

	ret := iv.process(data)
	return &ret
}

func (iv *nullableIntervalVector) Value(index uint) interface{} {
	val := iv.Get(index)
	if val != nil {
		return *val
	}
	return val
}

func processYear(val []byte) string {
	m := int32(binary.LittleEndian.Uint32(val))

	var prefix string
	if m < 0 {
		m = -m
		prefix = "-"
	}

	years := m / 12
	months := m % 12

	return fmt.Sprintf("%s%d-%d", prefix, years, months)
}

const daysToMillis = 24 * 60 * 60 * 1000

func processDay(val []byte) string {
	days := int32(binary.LittleEndian.Uint32(val))
	millis := int32(binary.LittleEndian.Uint32(val[4:]))

	isneg := (days < 0) || (days == 0 && millis < 0)
	if days < 0 {
		days = -days
	}
	if millis < 0 {
		millis = -millis
	}

	days += millis / daysToMillis
	millis = millis % daysToMillis

	dur := time.Duration(millis) * time.Millisecond
	var prefix string
	if isneg {
		prefix = "-"
	}

	return fmt.Sprintf("%s%d days %s", prefix, days, dur.String())
}

func processInterval(val []byte) string {
	m := int32(binary.LittleEndian.Uint32(val))
	days := int32(binary.LittleEndian.Uint32(val[4:]))
	millis := int32(binary.LittleEndian.Uint32(val[8:]))

	isneg := (m < 0) || (m == 0 && days < 0) || (m == 0 && days == 0 && millis < 0)
	m = int32(math.Abs(float64(m)))
	days = int32(math.Abs(float64(days)))
	millis = int32(math.Abs(float64(millis)))

	years := m / 12
	months := m % 12

	days += millis / daysToMillis
	millis = millis % daysToMillis

	dur := time.Duration(millis) * time.Millisecond

	var prefix string
	if isneg {
		prefix = "-"
	}

	return fmt.Sprintf("%s%d-%d-%d %s", prefix, years, months, days, dur.String())
}

func NewIntervalYearVector(data []byte, meta *shared.SerializedField) *intervalVector {
	return &intervalVector{
		intervalBase: &fixedWidthVec{data, 4, meta},
		process:      processYear,
	}
}

func NewNullableIntervalYearVector(data []byte, meta *shared.SerializedField) *nullableIntervalVector {
	return &nullableIntervalVector{
		newNullableFixedWidth(data, meta, 4),
		processYear,
	}
}

func NewIntervalDayVector(data []byte, meta *shared.SerializedField) *intervalVector {
	return &intervalVector{
		intervalBase: &fixedWidthVec{data, 8, meta},
		process:      processDay,
	}
}

func NewNullableIntervalDayVector(data []byte, meta *shared.SerializedField) *nullableIntervalVector {
	return &nullableIntervalVector{
		newNullableFixedWidth(data, meta, 8),
		processDay,
	}
}

func NewIntervalVector(data []byte, meta *shared.SerializedField) *intervalVector {
	return &intervalVector{
		intervalBase: &fixedWidthVec{data, 12, meta},
		process:      processInterval,
	}
}

func NewNullableIntervalVector(data []byte, meta *shared.SerializedField) *nullableIntervalVector {
	return &nullableIntervalVector{
		newNullableFixedWidth(data, meta, 12),
		processInterval,
	}
}
