package driver

import (
	"bytes"
	"compress/zlib"
	"database/sql/driver"
	"encoding/hex"
	"io"
	"io/ioutil"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeroshade/go-drill"
	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/common"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"google.golang.org/protobuf/proto"
)

func TestImplements(t *testing.T) {
	assert.Implements(t, (*driver.RowsColumnTypeDatabaseTypeName)(nil), new(rows))
	assert.Implements(t, (*driver.RowsColumnTypeLength)(nil), new(rows))
	assert.Implements(t, (*driver.RowsColumnTypeNullable)(nil), new(rows))
	assert.Implements(t, (*driver.RowsColumnTypePrecisionScale)(nil), new(rows))
	assert.Implements(t, (*driver.RowsColumnTypeScanType)(nil), new(rows))
}

func TestRowsClose(t *testing.T) {
	m := new(mockResHandle)
	m.Test(t)
	defer m.AssertExpectations(t)

	m.On("Close").Return(assert.AnError)

	r := &rows{handle: m}
	assert.Same(t, assert.AnError, r.Close())
}

func TestRowsGetCols(t *testing.T) {
	m := new(mockResHandle)
	m.Test(t)
	defer m.AssertExpectations(t)

	cols := []string{"a", "b", "c"}
	m.On("GetCols").Return(cols)

	r := &rows{handle: m}
	assert.Exactly(t, cols, r.Columns())
}

var compraw = "789c5c92cf6ed34010c6bfa4e5cf9113270ec311a90421240e705a5a132c15b78a7a29b7ad3d76962ebbeefe09f18977e24d780d5e802bcac68e93ecc19fb433dfcc6f678cfe4c7a9df67ad2eb69af8f7a7ddceb935e9f623c9bbb67009e037801e025805700de00780fe023007139cf16b9108b7956dce485f8b410dff2cb7351880b91cd6faf6fb29b2ff9d5752e3e2f44719ecdb3c55751dce6c5452e8e518ff51487e7e448a77bb177007e01f80be06a02fc9e007f26c0bf09f0760ab829404bd9349a67544ac775d4baa35a19a9a9e2d67a153c551cb80ce475a73b928d5452536dd7eca975f687f23c84cad2ba4a998682a5b06472dc442d5d0a4413fc8ceeacaec8f143641f3c496d4d97be8d571593ad93ad6593aa0c00fb68bee572d3bf95e5bd6cd893744cf2cec690acca59a34aaaad6bd8cf7aaec1c3d2d3529aa6cf3a23af349bb0ab35e40fd8073deae8948d5e776457ec52b3a0c272e3aa75ac6ba5bbf4ba6e43b3e294306247137d947a736b2bab5548beed98636363d8367988aabc4f9374d6fb5463ec3b60552b3623db4f79bff3cd52cee1168761cf06ffd9f6f99a5a2d036bde0bed1645ebd74e761f06ecb38304ef89d765f4eca994dfad1ed7bf831e27bafb895aa74c20e96c34ff030000ffff84ef0203"

var sampleDef = shared.RecordBatchDef{
	RecordCount: proto.Int32(9),
	Field: []*shared.SerializedField{
		{
			MajorType:    &common.MajorType{MinorType: common.MinorType_BIGINT.Enum(), Mode: common.DataMode_REQUIRED.Enum()},
			NamePart:     &shared.NamePart{Name: proto.String("N_NATIONKEY")},
			ValueCount:   proto.Int32(9),
			BufferLength: proto.Int32(72),
		},
		{
			MajorType: &common.MajorType{MinorType: common.MinorType_VARBINARY.Enum(), Mode: common.DataMode_REQUIRED.Enum()},
			NamePart:  &shared.NamePart{Name: proto.String("N_NAME")},
			Child: []*shared.SerializedField{
				{
					MajorType:    &common.MajorType{MinorType: common.MinorType_UINT4.Enum(), Mode: common.DataMode_REQUIRED.Enum()},
					NamePart:     &shared.NamePart{Name: proto.String("$offsets$")},
					ValueCount:   proto.Int32(10),
					BufferLength: proto.Int32(40),
				},
			},
			ValueCount:   proto.Int32(9),
			BufferLength: proto.Int32(99),
		},
		{
			MajorType:    &common.MajorType{MinorType: common.MinorType_BIGINT.Enum(), Mode: common.DataMode_REQUIRED.Enum()},
			NamePart:     &shared.NamePart{Name: proto.String("N_REGIONKEY")},
			ValueCount:   proto.Int32(9),
			BufferLength: proto.Int32(72),
		},
		{
			MajorType: &common.MajorType{MinorType: common.MinorType_VARBINARY.Enum(), Mode: common.DataMode_REQUIRED.Enum()},
			NamePart:  &shared.NamePart{Name: proto.String("N_COMMENT")},
			Child: []*shared.SerializedField{
				{
					MajorType:    &common.MajorType{MinorType: common.MinorType_UINT4.Enum(), Mode: common.DataMode_REQUIRED.Enum()},
					NamePart:     &shared.NamePart{Name: proto.String("$offsets$")},
					ValueCount:   proto.Int32(10),
					BufferLength: proto.Int32(40),
				},
			},
			ValueCount:   proto.Int32(9),
			BufferLength: proto.Int32(666),
		},
	},
}

func getSampleRecordBatch() *drill.RecordBatch {
	b, _ := hex.DecodeString(compraw)
	zr, _ := zlib.NewReader(bytes.NewReader(b))
	defer zr.Close()

	rawblock, _ := ioutil.ReadAll(zr)

	rb := &drill.RecordBatch{
		Def:  &sampleDef,
		Vecs: make([]data.DataVector, 0, len(sampleDef.GetField())),
	}

	var offset int32 = 0
	for _, f := range sampleDef.GetField() {
		rb.Vecs = append(rb.Vecs, data.NewValueVec(rawblock[offset:offset+f.GetBufferLength()], f))
		offset += f.GetBufferLength()
	}

	return rb
}

func TestRowsNext(t *testing.T) {
	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	mr.On("GetRecordBatch").Return(getSampleRecordBatch())

	r := &rows{handle: mr, curRow: 1}
	dest := make([]driver.Value, 4)

	assert.NoError(t, r.Next(dest))
	assert.Exactly(t, int64(1), dest[0])
	assert.Exactly(t, []byte("ARGENTINA"), dest[1])
	assert.Exactly(t, int64(1), dest[2])
	assert.Exactly(t, []byte("al foxes promise slyly according to the regular accounts. bold requests alon"), dest[3])
	assert.Equal(t, 2, r.curRow)
}

func TestRowsNextEnd(t *testing.T) {
	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	mr.On("GetRecordBatch").Return((*drill.RecordBatch)(nil))

	r := &rows{handle: mr, curRow: 1}
	dest := make([]driver.Value, 4)
	assert.Same(t, io.EOF, r.Next(dest))
}

func TestRowsNextCallNext(t *testing.T) {
	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	mr.On("GetRecordBatch").Return(getSampleRecordBatch())
	mr.On("Next").Return(nil, getSampleRecordBatch())

	r := &rows{handle: mr, curRow: 10}
	dest := make([]driver.Value, 4)
	assert.NoError(t, r.Next(dest))

	assert.Exactly(t, int64(0), dest[0])
	assert.Exactly(t, []byte("ALGERIA"), dest[1])
	assert.Exactly(t, int64(0), dest[2])
	assert.Exactly(t, []byte(" haggle. carefully final deposits detect slyly agai"), dest[3])
	assert.Equal(t, 1, r.curRow)
}

func TestRowsNextCallNextErr(t *testing.T) {
	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	mr.On("GetRecordBatch").Return(getSampleRecordBatch())
	mr.On("Next").Return(assert.AnError, (*drill.RecordBatch)(nil))

	r := &rows{handle: mr, curRow: 10}
	dest := make([]driver.Value, 4)
	assert.Same(t, assert.AnError, r.Next(dest))
}

func TestRowsColumnTypeHelpers(t *testing.T) {
	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	mr.On("GetRecordBatch").Return(getSampleRecordBatch())

	r := &rows{handle: mr, curRow: 0}

	tests := []struct {
		name string
		f    func() interface{}
		val  interface{}
	}{
		{"column type scan type", func() interface{} { return r.ColumnTypeScanType(0) }, reflect.TypeOf(int64(0))},
		{"column type scan type", func() interface{} { return r.ColumnTypeScanType(1) }, reflect.TypeOf([]byte{})},
		{"column type scan type", func() interface{} { return r.ColumnTypeScanType(2) }, reflect.TypeOf(int64(0))},
		{"column type scan type", func() interface{} { return r.ColumnTypeScanType(3) }, reflect.TypeOf([]byte{})},
		{"column database type name", func() interface{} { return r.ColumnTypeDatabaseTypeName(0) }, "BIGINT"},
		{"column database type name", func() interface{} { return r.ColumnTypeDatabaseTypeName(1) }, "VARBINARY"},
		{"column database type name", func() interface{} { return r.ColumnTypeDatabaseTypeName(2) }, "BIGINT"},
		{"column database type name", func() interface{} { return r.ColumnTypeDatabaseTypeName(3) }, "VARBINARY"},
		{"column type nullable", func() interface{} {
			a, b := r.ColumnTypeNullable(0)
			return []bool{a, b}
		}, []bool{false, true}},
		{"column type length", func() interface{} {
			a, b := r.ColumnTypeLength(0)
			return []interface{}{a, b}
		}, []interface{}{int64(0), false}},
		{"column type length", func() interface{} {
			a, b := r.ColumnTypeLength(1)
			return []interface{}{a, b}
		}, []interface{}{int64(math.MaxInt64), true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.f(), tt.val)
		})
	}
}
