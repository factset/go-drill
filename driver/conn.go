// Package driver provides a driver compatible with the golang database/sql/driver
// standard package.
//
// Basic example
//
//		import (
//			"strings"
//			"database/sql"
//
//			_ "github.com/factset/go-drill/driver"
//		)
//
//		func main() {
//			props := []string{
//				"zk=zookeeper1,zookeeper2,zookeeper3",
//				"auth=kerberos",
//				"service=<krb_service_name>",
//				"cluster=<clustername>",
//			}
//			db, err := sql.Open("drill", strings.Join(props, ";"))
//			...
//		}
//
// Connection String
//
// 		zk=node1,node2,node3/non/default/path
//				Specify the zookeeper nodes to utilize for discovering endpoints. Will
//				default to using port 2181 if not specified with the address for each
//				zookeeper node. Will default to using /drill/drillbits unless a non default
//				zookeeper path is specified as shown.
//
//		auth=<auth type>
//				If using sasl authentication, this is used to specify the authentication
// 				mechanism. Currently only supports "kerberos" which will use GSSAPI
// 				authentication. If using user/password authentication, this is ignored.
//
//		schema=<default schema>
//				Default schema/context to run queries in.
//
//		service=<servicename>
//				If using kerberos authentication, this should be the kerberos service name
//				for the ticket utilized by the server for authentication.
//
//		encrypt=<true|false>
//				Set to true if using Sasl Encryption for communication.
//
//		user=<username>
//				Username to authenticate as either for the kerberos TKT to use for auth,
//				or the username to authenticate with the provided password.
//
//		pass=<password>
//				If using user/pass authentication instead of kerberos, this is how you provide
//				the password to use.
//
//		cluster=<clustername>
//				If using a non-default cluster name for drill, specify this so that the
//				zookeeper cluster can be found properly for the drillbit endpoints.
//
//		host=<hostname>
//				Hostname to connect to for direct connection.
//
//		port=<portnumber>
//				Port number to use if not using the default 31010 port for direct connection.
//
//		heartbeat=<frequency in seconds>
//				By default the driver will use a 15 second heartbeat frequency to keep the
//				the connection going. If a different frequency is desired it can be specified
//				with this parameter. A frequency of 0 results in no heartbeat used.
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/factset/go-drill"
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
		var batch drill.RowBatch
		for batch, err = h.Next(); err == nil; batch, err = h.Next() {
			affectedRows += int64(batch.AffectedRows())
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
