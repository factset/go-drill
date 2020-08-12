package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"

	"github.com/zeroshade/go-drill"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
)

func init() {
	sql.Register("drill", Driver{})
}

type Driver struct{}

func (d Driver) Open(dsn string) (driver.Conn, error) {
	cn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}

	return cn.Connect(context.Background())
}

func (d Driver) OpenConnector(name string) (driver.Connector, error) {
	return parseConnectStr(name)
}

type conn struct {
	drill.Client
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.Client.PrepareQuery(query)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errors.New("drill driver does not support prepared statements")
	}

	handle, err := c.Client.SubmitQuery(shared.QueryType_SQL, query)
	if err != nil {
		log.Println(err)
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
