package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"
	"time"

	"github.com/factset/go-drill"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConnImplements(t *testing.T) {
	// verify we implement the interfaces that aren't automatically going to
	// be enforced by the compiler so that we don't mess up any of the functions
	assert.Implements(t, (*driver.Pinger)(nil), new(conn))
	assert.Implements(t, (*driver.QueryerContext)(nil), new(conn))
	assert.Implements(t, (*driver.ExecerContext)(nil), new(conn))
}

func TestDriverOpenErr(t *testing.T) {
	c, err := drillDriver{}.Open(";")
	assert.Nil(t, c)
	assert.Error(t, err)
}

func TestDriverOpenConnector(t *testing.T) {
	c, err := drillDriver{}.OpenConnector("auth=plain")
	assert.Equal(t, "plain", c.(*connector).base.(*drill.Client).Opts.Auth)
	assert.NoError(t, err)
}

func TestConnBegin(t *testing.T) {
	c := &conn{nil}
	tx, err := c.Begin()
	assert.Nil(t, tx)
	assert.EqualError(t, err, "not implemented")
}

func TestConnPrepare(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("PrepareQuery", "foobar").Return(p, nil)

	c := &conn{m}

	stmt, err := c.Prepare("foobar")
	assert.Equal(t, &prepared{stmt: p, client: m}, stmt)
	assert.NoError(t, err)
}

func TestConnPrepareErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	p := drill.PreparedHandle(5)
	m.On("PrepareQuery", "foobar").Return(p, assert.AnError)

	c := &conn{m}
	stmt, err := c.Prepare("foobar")
	assert.Nil(t, stmt)
	assert.Same(t, assert.AnError, err)
}

func TestConnQueryContextNoPrep(t *testing.T) {
	c := &conn{}

	rows, err := c.QueryContext(context.Background(), "foo", []driver.NamedValue{{Name: "foo"}})
	assert.Nil(t, rows)
	assert.Same(t, errNoPrepSupport, err)
}

func TestConnExecContextNoPrep(t *testing.T) {
	c := &conn{}

	rows, err := c.ExecContext(context.Background(), "foo", []driver.NamedValue{{Name: "foo"}})
	assert.Nil(t, rows)
	assert.Same(t, errNoPrepSupport, err)
}

func TestConnQueryContextErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	m.On("SubmitQuery", drill.TypeSQL, "foobar").Return((*drill.ResultHandle)(nil), assert.AnError)

	c := &conn{m}
	rows, err := c.QueryContext(context.Background(), "foobar", []driver.NamedValue{})
	assert.Nil(t, rows)
	assert.Same(t, driver.ErrBadConn, err)
}

func TestConnExecContextErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	m.On("SubmitQuery", drill.TypeSQL, "foobar").Return((*drill.ResultHandle)(nil), assert.AnError)

	c := &conn{m}
	rows, err := c.ExecContext(context.Background(), "foobar", []driver.NamedValue{})
	assert.Nil(t, rows)
	assert.Same(t, driver.ErrBadConn, err)
}

type mockResHandle struct {
	mock.Mock
}

func (m *mockResHandle) Cancel()           { m.Called() }
func (m *mockResHandle) Close() error      { return m.Called().Error(0) }
func (m *mockResHandle) GetCols() []string { return m.Called().Get(0).([]string) }
func (m *mockResHandle) GetRecordBatch() drill.RowBatch {
	ret := m.Called().Get(0)
	if ret == nil {
		return nil
	}
	return ret.(drill.RowBatch)
}
func (m *mockResHandle) Next() (drill.RowBatch, error) {
	args := m.Called()
	if args.Get(1) == nil {
		return nil, args.Error(0)
	}
	return args.Get(1).(drill.RowBatch), args.Error(0)
}

func TestConnQueryContextCtxTimeout(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	m.On("SubmitQuery", drill.TypeSQL, "foobar").Return(mr, nil)

	waiter := make(chan time.Time)
	mr.On("Cancel").Run(func(mock.Arguments) {
		waiter <- time.Now()
	})
	mr.On("Next").WaitUntil(waiter).Return(assert.AnError, (drill.RowBatch)(nil))

	c := &conn{m}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.QueryContext(ctx, "foobar", []driver.NamedValue{})
	assert.Same(t, assert.AnError, err)
}

func TestConnExecContextCtxTimeout(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	m.On("SubmitQuery", drill.TypeSQL, "foobar").Return(mr, nil)

	waiter := make(chan time.Time)
	mr.On("Cancel").Run(func(mock.Arguments) {
		waiter <- time.Now()
	})
	mr.On("Next").WaitUntil(waiter).Return(assert.AnError, (drill.RowBatch)(nil))

	c := &conn{m}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	r, err := c.ExecContext(ctx, "foobar", []driver.NamedValue{})
	assert.NoError(t, err)

	_, err = r.RowsAffected()
	assert.Same(t, assert.AnError, err)
}

func TestConnQueryContext(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	m.On("SubmitQuery", drill.TypeSQL, "foobar").Return(mr, nil)
	mr.On("Next").After(100*time.Millisecond).Return(nil, (drill.RowBatch)(nil))

	c := &conn{m}
	r, err := c.QueryContext(context.Background(), "foobar", []driver.NamedValue{})
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.IsType(t, &rows{}, r)
}

func TestConnExecContext(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	m.On("SubmitQuery", drill.TypeSQL, "foobar").Return(mr, nil)

	rb := new(mockBatch)
	rb.On("AffectedRows").Return(5)

	mr.On("Next").Return(nil, rb).Twice()
	mr.On("Next").Return(io.EOF, (drill.RowBatch)(nil))

	c := &conn{m}
	r, err := c.ExecContext(context.Background(), "foobar", []driver.NamedValue{})
	assert.NoError(t, err)
	num, err := r.RowsAffected()
	assert.NoError(t, err)
	assert.EqualValues(t, 10, num)
}

func TestConnExecContextWithErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)
	defer m.AssertExpectations(t)

	mr := new(mockResHandle)
	mr.Test(t)
	defer mr.AssertExpectations(t)

	m.On("SubmitQuery", drill.TypeSQL, "foobar").Return(mr, nil)

	rb := new(mockBatch)
	rb.On("AffectedRows").Return(5)

	mr.On("Next").Return(nil, rb).Twice()
	mr.On("Next").Return(assert.AnError, (drill.RowBatch)(nil))

	c := &conn{m}
	r, err := c.ExecContext(context.Background(), "foobar", []driver.NamedValue{})
	assert.NoError(t, err)
	num, err := r.RowsAffected()
	assert.Same(t, assert.AnError, err)
	assert.EqualValues(t, 10, num)
}
