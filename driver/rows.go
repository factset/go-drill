package driver

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/zeroshade/go-drill"
	"github.com/zeroshade/go-drill/internal/rpc/proto/common"
)

type rows struct {
	handle *drill.ResultHandle
	curRow int
}

func (r *rows) Close() error {
	return r.handle.Close()
}

func (r *rows) Columns() []string {
	return r.handle.GetCols()
}

func (r *rows) Next(dest []driver.Value) error {
	rb := r.handle.GetRecordBatch()
	if rb == nil {
		return io.EOF
	}

	if int32(r.curRow) >= *rb.Def.RecordCount {
		err := r.handle.Next()
		if err == drill.QueryCompleted {
			return io.EOF
		}

		if err != nil {
			return err
		}

		r.curRow = 0
		rb = r.handle.GetRecordBatch()
	}

	for i := range dest {
		dest[i] = rb.Vecs[i].Value(uint(r.curRow))
	}

	r.curRow++
	return nil
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	return r.handle.GetRecordBatch().Vecs[index].Type()
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.handle.GetRecordBatch().Def.Field[index].MajorType.GetMode() == common.DataMode_OPTIONAL, true
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.handle.GetRecordBatch().Def.Field[index].MajorType.GetMinorType().String()
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	typ := r.handle.GetRecordBatch().Def.Field[index].GetMajorType()
	switch typ.GetMinorType() {
	case common.MinorType_VARCHAR:
	case common.MinorType_VAR16CHAR:
	case common.MinorType_VARBINARY:
	case common.MinorType_VARDECIMAL:
		length = int64(typ.GetWidth())
		ok = true
	}
	return
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	typ := r.handle.GetRecordBatch().Def.Field[index].GetMajorType()
	switch typ.GetMinorType() {
	case common.MinorType_DECIMAL9:
	case common.MinorType_DECIMAL18:
	case common.MinorType_DECIMAL28SPARSE:
	case common.MinorType_DECIMAL38SPARSE:
	case common.MinorType_MONEY:
	case common.MinorType_FLOAT4:
	case common.MinorType_FLOAT8:
	case common.MinorType_DECIMAL28DENSE:
	case common.MinorType_DECIMAL38DENSE:
		precision = int64(typ.GetPrecision())
		scale = int64(typ.GetScale())
		ok = true
	}
	return
}
