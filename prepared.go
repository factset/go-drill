package drill

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
)

type prepared struct {
	stmt *user.PreparedStatement

	client *drillClient
}

func (p *prepared) Close() error {
	p.stmt = nil
	p.client = nil
	return nil
}

func (p *prepared) NumInput() int {
	// drill does not support parameters for prepared statements
	return 0
}

func (p *prepared) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec is deprecated, use ExecContext instead")
}
func (p *prepared) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Query is deprecated, use QueryContext instead")
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
		return nil, fmt.Errorf("drill does not support parameters in prepared statements")
	}

	handle, err := p.client.ExecuteStmt(p.stmt)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			handle.Cancel()
		case <-done:
		}
	}()

	var affectedRows int64 = 0
	for err = handle.Next(); err != nil; err = handle.Next() {
		batch := handle.GetRecordBatch()

		affectedRows += int64(batch.Def.GetAffectedRowsCount())
	}

	if err == QueryCompleted {
		err = nil
	}

	return driver.ResultNoRows, err
}

func (p *prepared) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("drill does not support parameters in prepared statements")
	}

	handle, err := p.client.ExecuteStmt(p.stmt)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			handle.Cancel()
		case <-done:
		}
	}()

	r := &rows{
		handle: handle,
	}

	err = handle.Next()
	close(done)
	return r, err
}
