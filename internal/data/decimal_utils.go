package data

import (
	"encoding/binary"
	"math"
	"math/big"
)

type DecimalTraits interface {
	NumDigits() int
	ByteWidth() int
	IsSparse() bool
	MaxPrecision() int
}

var (
	Decimal28DenseTraits  decimal28DenseTraits
	Decimal38DenseTraits  decimal38DenseTraits
	Decimal28SparseTraits decimal28SparseTraits
	Decimal38SparseTraits decimal38SparseTraits
)

type decimal28DenseTraits struct{}

func (decimal28DenseTraits) NumDigits() int    { return 3 }
func (decimal28DenseTraits) ByteWidth() int    { return 12 }
func (decimal28DenseTraits) IsSparse() bool    { return false }
func (decimal28DenseTraits) MaxPrecision() int { return 28 }

type decimal38DenseTraits struct{}

func (decimal38DenseTraits) NumDigits() int    { return 4 }
func (decimal38DenseTraits) ByteWidth() int    { return 16 }
func (decimal38DenseTraits) IsSparse() bool    { return false }
func (decimal38DenseTraits) MaxPrecision() int { return 38 }

type decimal28SparseTraits struct{}

func (decimal28SparseTraits) NumDigits() int    { return 5 }
func (decimal28SparseTraits) ByteWidth() int    { return 20 }
func (decimal28SparseTraits) IsSparse() bool    { return true }
func (decimal28SparseTraits) MaxPrecision() int { return 28 }

type decimal38SparseTraits struct{}

func (decimal38SparseTraits) NumDigits() int    { return 6 }
func (decimal38SparseTraits) ByteWidth() int    { return 24 }
func (decimal38SparseTraits) IsSparse() bool    { return true }
func (decimal38SparseTraits) MaxPrecision() int { return 38 }

const (
	maxdigits = 9
	digBase   = 1000000000
)

var base = big.NewFloat(digBase)

func getFloatFromBytes(valbytes []byte, digits, scale int, truncate bool) *big.Float {
	// sparse types (truncate == true) are little endian, otherwise we're big endian
	var order binary.ByteOrder
	if truncate {
		order = binary.LittleEndian
	} else {
		order = binary.BigEndian
	}

	val := big.NewFloat(float64(order.Uint32(valbytes) & 0x7FFFFFFF))
	for i := 1; i < digits; i++ {
		tmp := big.NewFloat(float64(order.Uint32(valbytes[i*Uint32SizeBytes:])))
		val.Mul(val, base)
		val.Add(val, tmp)
	}

	actualDigits := int32(scale % maxdigits)
	if truncate && scale > 0 && (actualDigits != 0) {
		val.Quo(val, big.NewFloat(math.Pow10(int(maxdigits-actualDigits))))
	}

	if order.Uint32(valbytes)&0x80000000 != 0 {
		val.Neg(val)
	}

	// scale it and return it
	return val.Quo(val, big.NewFloat(math.Pow10(int(scale))))
}
