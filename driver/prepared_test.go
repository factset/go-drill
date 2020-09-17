package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"
	"time"

	"github.com/factset/go-drill"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPreparedImplements(t *testing.T) {
	assert.Implements(t, (*driver.StmtExecContext)(nil), new(prepared))
	assert.Implements(t, (*driver.StmtQueryContext)(nil), new(prepared))
}

func TestPreparedClose(t *testing.T) {
	prep := &prepared{stmt: drill.PreparedHandle(5), client: new(mockDrillClient)}
	assert.NoError(t, prep.Close())
	assert.Nil(t, prep.stmt)
	assert.Nil(t, prep.client)
}

func TestPreparedNumInput(t *testing.T) {
	p := &prepared{}
	assert.Zero(t, p.NumInput())
}

func TestPreparedExec(t *testing.T) {
	p := &prepared{}
	r, e := p.Exec([]driver.Value{})
	assert.Equal(t, driver.ResultNoRows, r)
	assert.Same(t, driver.ErrSkip, e)
}

func TestPreparedQuery(t *testing.T) {
	p := &prepared{}
	r, e := p.Query([]driver.Value{})
	assert.Nil(t, r)
	assert.Same(t, driver.ErrSkip, e)
}

func TestPreparedExecContext(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("ExecuteStmt", p).Return(mr, nil)

	rb := &drill.RecordBatch{
		Def: &shared.RecordBatchDef{
			AffectedRowsCount: proto.Int32(5),
		},
	}

	mr.On("Next").Return(nil, rb).Twice()
	mr.On("Next").Return(io.EOF, (*drill.RecordBatch)(nil))

	prep := &prepared{stmt: p, client: m}
	r, err := prep.ExecContext(context.Background(), []driver.NamedValue{})
	assert.NoError(t, err)
	num, err := r.RowsAffected()
	assert.NoError(t, err)
	assert.EqualValues(t, 10, num)

	_, err = r.LastInsertId()
	assert.Error(t, err)
}

func TestPreparedExecContextWithErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("ExecuteStmt", p).Return(mr, nil)

	rb := &drill.RecordBatch{
		Def: &shared.RecordBatchDef{
			AffectedRowsCount: proto.Int32(5),
		},
	}

	mr.On("Next").Return(nil, rb).Twice()
	mr.On("Next").Return(assert.AnError, (*drill.RecordBatch)(nil))

	prep := &prepared{stmt: p, client: m}
	r, err := prep.ExecContext(context.Background(), []driver.NamedValue{})
	assert.NoError(t, err)
	num, err := r.RowsAffected()
	assert.Same(t, assert.AnError, err)
	assert.EqualValues(t, 10, num)
}

func TestPreparedExecContextCtxTimeout(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("ExecuteStmt", p).Return(mr, nil)

	waiter := make(chan time.Time)
	mr.On("Cancel").Run(func(mock.Arguments) {
		waiter <- time.Now()
	})
	mr.On("Next").WaitUntil(waiter).Return(assert.AnError, (*drill.RecordBatch)(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	prep := &prepared{stmt: p, client: m}
	r, err := prep.ExecContext(ctx, []driver.NamedValue{})
	assert.NoError(t, err)
	_, err = r.RowsAffected()
	assert.Same(t, assert.AnError, err)
}

func TestPreparedExecContextErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("ExecuteStmt", p).Return((*drill.ResultHandle)(nil), assert.AnError)

	prep := &prepared{stmt: p, client: m}
	r, err := prep.ExecContext(context.Background(), []driver.NamedValue{})
	assert.Same(t, driver.ErrBadConn, err)
	assert.Nil(t, r)
}

func TestPreparedExecContextNoPrep(t *testing.T) {
	prep := &prepared{}
	r, err := prep.ExecContext(context.Background(), []driver.NamedValue{{Name: "foobar"}})
	assert.Nil(t, r)
	assert.Same(t, errNoPrepSupport, err)
}

func TestPreparedQueryContextNoPrep(t *testing.T) {
	prep := &prepared{}
	r, err := prep.QueryContext(context.Background(), []driver.NamedValue{{Name: "foobar"}})
	assert.Nil(t, r)
	assert.Same(t, errNoPrepSupport, err)
}

func TestPreparedQueryContextErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("ExecuteStmt", p).Return((*drill.ResultHandle)(nil), assert.AnError)

	prep := &prepared{stmt: p, client: m}
	r, err := prep.QueryContext(context.Background(), []driver.NamedValue{})
	assert.Same(t, driver.ErrBadConn, err)
	assert.Nil(t, r)
}

func TestPreparedQueryContextCtxTimeout(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("ExecuteStmt", p).Return(mr, nil)

	waiter := make(chan time.Time)
	mr.On("Cancel").Run(func(mock.Arguments) {
		waiter <- time.Now()
	})
	mr.On("Next").WaitUntil(waiter).Return(assert.AnError, (*drill.RecordBatch)(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	prep := &prepared{stmt: p, client: m}
	_, err := prep.QueryContext(ctx, []driver.NamedValue{})
	assert.Same(t, assert.AnError, err)
}

func TestPreparedQueryContext(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("ExecuteStmt", p).Return(mr, nil)
	mr.On("Next").Return(nil, (*drill.RecordBatch)(nil))

	prep := &prepared{stmt: p, client: m}
	r, err := prep.QueryContext(context.Background(), []driver.NamedValue{})
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.IsType(t, &rows{}, r)
}
