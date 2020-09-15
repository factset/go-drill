package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/zeroshade/go-drill"
)

var errNoPrepSupport = errors.New("drill does not support parameters in prepared statements")

func init() {
	sql.Register("drill", drillDriver{})
}

type drillDriver struct{}

func (d drillDriver) Open(dsn string) (driver.Conn, error) {
	cn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}

	return cn.Connect(context.Background())
}

func (d drillDriver) OpenConnector(name string) (driver.Connector, error) {
	return parseConnectStr(name)
}

func processWithCtx(ctx context.Context, handle drill.DataHandler, f func(h drill.DataHandler) error) error {
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			handle.Cancel()
		case <-done:
		}
	}()

	return f(handle)
}

type conn struct {
	drill.Conn
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.PrepareQuery(query)
	if err != nil {
		return nil, err
	}
	return &prepared{stmt: stmt, client: c.Conn}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, errNoPrepSupport
	}

	handle, err := c.Conn.SubmitQuery(drill.TypeSQL, query)
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

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errNoPrepSupport
	}

	handle, err := c.Conn.SubmitQuery(drill.TypeSQL, query)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	r := &rows{handle: handle}
	return r, processWithCtx(ctx, handle, func(h drill.DataHandler) error {
		_, err := h.Next()
		return err
	})
}
