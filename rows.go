package drill

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/common"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
)

type rowbatch struct {
	def         *shared.RecordBatchDef
	vecs        []data.DataVector
	queryResult *shared.QueryResult
	readErr     error
}

type rows struct {
	batches     chan *rowbatch
	columnNames []string

	curBatch *rowbatch
	curRow   int
}

func (r *rows) ensureCurBatch() {
	if r.curBatch == nil {
		r.curBatch = <-r.batches
	}
}

func (r *rows) Close() error {
	if r.curBatch != nil && r.curBatch.readErr != nil {
		return driver.ErrBadConn
	}

	r.batches = nil
	r.curBatch = nil
	r.curRow = -1
	r.columnNames = nil
	return nil
}

func (r *rows) Columns() []string {
	if r.columnNames == nil {
		if r.curBatch == nil {
			r.curBatch = <-r.batches
		}

		r.columnNames = make([]string, len(r.curBatch.def.GetField()))
		for idx, f := range r.curBatch.def.GetField() {
			r.columnNames[idx] = *f.NamePart.Name
		}
	}

	return r.columnNames
}

func (r *rows) getResult() error {
	if r.curBatch.queryResult != nil {
		switch r.curBatch.queryResult.GetQueryState() {
		case shared.QueryResult_COMPLETED:
			return io.EOF
		case shared.QueryResult_CANCELED:
			return errors.New("query cancelled")
		case shared.QueryResult_FAILED:
			return fmt.Errorf("query failed with error: %s", r.curBatch.queryResult.GetError()[0].GetMessage())
		}
	}

	if r.curBatch.readErr != nil {
		return r.curBatch.readErr
	}

	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.curBatch != nil {
		if err := r.getResult(); err != nil {
			return err
		}
	}

	if r.curBatch == nil || r.curRow < 0 || int32(r.curRow) >= r.curBatch.def.GetRecordCount() {
		var ok bool
		r.curBatch, ok = <-r.batches
		if !ok {
			return io.EOF
		}
	}

	if err := r.getResult(); err != nil {
		return err
	}

	for i := range dest {
		dest[i] = r.curBatch.vecs[i].Value(uint(r.curRow))
	}

	r.curRow++
	return nil
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	r.ensureCurBatch()

	return r.curBatch.vecs[index].Type()
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	r.ensureCurBatch()

	return r.curBatch.def.Field[index].MajorType.GetMode() == common.DataMode_OPTIONAL, true
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	r.ensureCurBatch()

	return r.curBatch.def.Field[index].MajorType.GetMinorType().String()
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	r.ensureCurBatch()

	typ := r.curBatch.def.Field[index].GetMajorType()
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
	r.ensureCurBatch()

	typ := r.curBatch.def.Field[index].GetMajorType()
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
