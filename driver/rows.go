package driver

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/factset/go-drill"
	"github.com/factset/go-drill/internal/rpc/proto/common"
)

type rows struct {
	handle drill.DataHandler
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

	if int32(r.curRow) >= rb.Def.GetRecordCount() {
		var err error
		rb, err = r.handle.Next()
		if err != nil {
			return err
		}

		r.curRow = 0
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
	return r.handle.GetRecordBatch().Def.GetField()[index].MajorType.GetMode() == common.DataMode_OPTIONAL, true
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.handle.GetRecordBatch().Def.GetField()[index].MajorType.GetMinorType().String()
}

func (r *rows) ColumnTypeLength(index int) (int64, bool) {
	return r.handle.GetRecordBatch().Vecs[index].TypeLen()
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	typ := r.handle.GetRecordBatch().Def.GetField()[index].GetMajorType()
	switch typ.GetMinorType() {
	case common.MinorType_DECIMAL9,
		common.MinorType_DECIMAL18,
		common.MinorType_DECIMAL28SPARSE,
		common.MinorType_DECIMAL38SPARSE,
		common.MinorType_MONEY,
		common.MinorType_FLOAT4,
		common.MinorType_FLOAT8,
		common.MinorType_DECIMAL28DENSE,
		common.MinorType_DECIMAL38DENSE:

		precision = int64(typ.GetPrecision())
		scale = int64(typ.GetScale())
		ok = true
	}
	return
}
