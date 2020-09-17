package driver

import (
	"context"
	"database/sql/driver"
	"io"

	"github.com/factset/go-drill"
)

type prepared struct {
	stmt drill.PreparedHandle

	client drill.Conn
}

func (p *prepared) Close() error {
	p.stmt = nil
	p.client = nil
	return nil
}

func (p *prepared) NumInput() int {
	return 0
}

func (p *prepared) Exec(args []driver.Value) (driver.Result, error) {
	return driver.ResultNoRows, driver.ErrSkip
}

func (p *prepared) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

type result struct {
	rowsAffected int64
	rowsError    error
}

func (r result) LastInsertId() (int64, error) {
	return driver.ResultNoRows.LastInsertId()
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected, r.rowsError
}

func (p *prepared) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, errNoPrepSupport
	}

	handle, err := p.client.ExecuteStmt(p.stmt)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	var affectedRows int64 = 0
	err = processWithCtx(ctx, handle, func(h drill.DataHandler) error {
		var err error
		var batch *drill.RecordBatch
		for batch, err = h.Next(); err == nil; batch, err = h.Next() {
			affectedRows += int64(batch.Def.GetAffectedRowsCount())
		}

		return err
	})

	if err == io.EOF {
		err = nil
	}

	return result{rowsAffected: affectedRows, rowsError: err}, nil
}

func (p *prepared) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errNoPrepSupport
	}

	handle, err := p.client.ExecuteStmt(p.stmt)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	r := &rows{handle: handle}
	return r, processWithCtx(ctx, handle, func(h drill.DataHandler) error {
		_, err := h.Next()
		return err
	})
}
