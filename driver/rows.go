package driver

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/factset/go-drill"
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

	if int32(r.curRow) >= rb.NumRows() {
		var err error
		rb, err = r.handle.Next()
		if err != nil {
			return err
		}

		r.curRow = 0
	}

	src := rb.GetVectors()
	for i := range dest {
		dest[i] = src[i].Value(uint(r.curRow))
	}

	r.curRow++
	return nil
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	return r.handle.GetRecordBatch().GetVectors()[index].Type()
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.handle.GetRecordBatch().IsNullable(index), true
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.handle.GetRecordBatch().TypeName(index)
}

func (r *rows) ColumnTypeLength(index int) (int64, bool) {
	return r.handle.GetRecordBatch().GetVectors()[index].TypeLen()
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return r.handle.GetRecordBatch().PrecisionScale(index)
}
