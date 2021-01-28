// Code generated by type_traits_numeric.gen.go.tmpl. DO NOT EDIT.

package data

import (
	"reflect"
	"unsafe"
)

var (
	Int64Traits   int64Traits
	Int32Traits   int32Traits
	Float64Traits float64Traits
	Uint64Traits  uint64Traits
	Uint32Traits  uint32Traits
	Float32Traits float32Traits
	Int16Traits   int16Traits
	Uint16Traits  uint16Traits
	Int8Traits    int8Traits
	Uint8Traits   uint8Traits
)

// Int64 traits

const (
	// Int64SizeBytes specifies the number of bytes required to store a single int64 in memory
	Int64SizeBytes = int(unsafe.Sizeof(int64(0)))
)

type int64Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (int64Traits) BytesRequired(n int) int { return Int64SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type int64
//
// NOTE: len(b) must be a multiple of Int64SizeBytes
func (int64Traits) CastFromBytes(b []byte) []int64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int64SizeBytes
	s.Cap = h.Cap / Int64SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (int64Traits) CastToBytes(b []int64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int64SizeBytes
	s.Cap = h.Cap * Int64SizeBytes

	return res
}

// Copy copies src to dst
func (int64Traits) Copy(dst, src []int64) { copy(dst, src) }

// Int32 traits

const (
	// Int32SizeBytes specifies the number of bytes required to store a single int32 in memory
	Int32SizeBytes = int(unsafe.Sizeof(int32(0)))
)

type int32Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (int32Traits) BytesRequired(n int) int { return Int32SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type int32
//
// NOTE: len(b) must be a multiple of Int32SizeBytes
func (int32Traits) CastFromBytes(b []byte) []int32 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int32
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int32SizeBytes
	s.Cap = h.Cap / Int32SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (int32Traits) CastToBytes(b []int32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int32SizeBytes
	s.Cap = h.Cap * Int32SizeBytes

	return res
}

// Copy copies src to dst
func (int32Traits) Copy(dst, src []int32) { copy(dst, src) }

// Float64 traits

const (
	// Float64SizeBytes specifies the number of bytes required to store a single float64 in memory
	Float64SizeBytes = int(unsafe.Sizeof(float64(0)))
)

type float64Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (float64Traits) BytesRequired(n int) int { return Float64SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type float64
//
// NOTE: len(b) must be a multiple of Float64SizeBytes
func (float64Traits) CastFromBytes(b []byte) []float64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []float64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Float64SizeBytes
	s.Cap = h.Cap / Float64SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (float64Traits) CastToBytes(b []float64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Float64SizeBytes
	s.Cap = h.Cap * Float64SizeBytes

	return res
}

// Copy copies src to dst
func (float64Traits) Copy(dst, src []float64) { copy(dst, src) }

// Uint64 traits

const (
	// Uint64SizeBytes specifies the number of bytes required to store a single uint64 in memory
	Uint64SizeBytes = int(unsafe.Sizeof(uint64(0)))
)

type uint64Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (uint64Traits) BytesRequired(n int) int { return Uint64SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type uint64
//
// NOTE: len(b) must be a multiple of Uint64SizeBytes
func (uint64Traits) CastFromBytes(b []byte) []uint64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint64SizeBytes
	s.Cap = h.Cap / Uint64SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (uint64Traits) CastToBytes(b []uint64) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint64SizeBytes
	s.Cap = h.Cap * Uint64SizeBytes

	return res
}

// Copy copies src to dst
func (uint64Traits) Copy(dst, src []uint64) { copy(dst, src) }

// Uint32 traits

const (
	// Uint32SizeBytes specifies the number of bytes required to store a single uint32 in memory
	Uint32SizeBytes = int(unsafe.Sizeof(uint32(0)))
)

type uint32Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (uint32Traits) BytesRequired(n int) int { return Uint32SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type uint32
//
// NOTE: len(b) must be a multiple of Uint32SizeBytes
func (uint32Traits) CastFromBytes(b []byte) []uint32 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint32
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint32SizeBytes
	s.Cap = h.Cap / Uint32SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (uint32Traits) CastToBytes(b []uint32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint32SizeBytes
	s.Cap = h.Cap * Uint32SizeBytes

	return res
}

// Copy copies src to dst
func (uint32Traits) Copy(dst, src []uint32) { copy(dst, src) }

// Float32 traits

const (
	// Float32SizeBytes specifies the number of bytes required to store a single float32 in memory
	Float32SizeBytes = int(unsafe.Sizeof(float32(0)))
)

type float32Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (float32Traits) BytesRequired(n int) int { return Float32SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type float32
//
// NOTE: len(b) must be a multiple of Float32SizeBytes
func (float32Traits) CastFromBytes(b []byte) []float32 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []float32
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Float32SizeBytes
	s.Cap = h.Cap / Float32SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (float32Traits) CastToBytes(b []float32) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Float32SizeBytes
	s.Cap = h.Cap * Float32SizeBytes

	return res
}

// Copy copies src to dst
func (float32Traits) Copy(dst, src []float32) { copy(dst, src) }

// Int16 traits

const (
	// Int16SizeBytes specifies the number of bytes required to store a single int16 in memory
	Int16SizeBytes = int(unsafe.Sizeof(int16(0)))
)

type int16Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (int16Traits) BytesRequired(n int) int { return Int16SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type int16
//
// NOTE: len(b) must be a multiple of Int16SizeBytes
func (int16Traits) CastFromBytes(b []byte) []int16 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int16
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int16SizeBytes
	s.Cap = h.Cap / Int16SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (int16Traits) CastToBytes(b []int16) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int16SizeBytes
	s.Cap = h.Cap * Int16SizeBytes

	return res
}

// Copy copies src to dst
func (int16Traits) Copy(dst, src []int16) { copy(dst, src) }

// Uint16 traits

const (
	// Uint16SizeBytes specifies the number of bytes required to store a single uint16 in memory
	Uint16SizeBytes = int(unsafe.Sizeof(uint16(0)))
)

type uint16Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (uint16Traits) BytesRequired(n int) int { return Uint16SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type uint16
//
// NOTE: len(b) must be a multiple of Uint16SizeBytes
func (uint16Traits) CastFromBytes(b []byte) []uint16 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint16
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint16SizeBytes
	s.Cap = h.Cap / Uint16SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (uint16Traits) CastToBytes(b []uint16) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint16SizeBytes
	s.Cap = h.Cap * Uint16SizeBytes

	return res
}

// Copy copies src to dst
func (uint16Traits) Copy(dst, src []uint16) { copy(dst, src) }

// Int8 traits

const (
	// Int8SizeBytes specifies the number of bytes required to store a single int8 in memory
	Int8SizeBytes = int(unsafe.Sizeof(int8(0)))
)

type int8Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (int8Traits) BytesRequired(n int) int { return Int8SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type int8
//
// NOTE: len(b) must be a multiple of Int8SizeBytes
func (int8Traits) CastFromBytes(b []byte) []int8 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []int8
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int8SizeBytes
	s.Cap = h.Cap / Int8SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (int8Traits) CastToBytes(b []int8) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int8SizeBytes
	s.Cap = h.Cap * Int8SizeBytes

	return res
}

// Copy copies src to dst
func (int8Traits) Copy(dst, src []int8) { copy(dst, src) }

// Uint8 traits

const (
	// Uint8SizeBytes specifies the number of bytes required to store a single uint8 in memory
	Uint8SizeBytes = int(unsafe.Sizeof(uint8(0)))
)

type uint8Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory
func (uint8Traits) BytesRequired(n int) int { return Uint8SizeBytes * n }

// CastFromBytes reinterprets the slice b to a slice of type uint8
//
// NOTE: len(b) must be a multiple of Uint8SizeBytes
func (uint8Traits) CastFromBytes(b []byte) []uint8 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint8
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Uint8SizeBytes
	s.Cap = h.Cap / Uint8SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (uint8Traits) CastToBytes(b []uint8) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Uint8SizeBytes
	s.Cap = h.Cap * Uint8SizeBytes

	return res
}

// Copy copies src to dst
func (uint8Traits) Copy(dst, src []uint8) { copy(dst, src) }
