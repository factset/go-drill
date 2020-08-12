package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"

	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
)

func init() {
	sql.Register("drill", &Driver{})
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

func (d *drillClient) Prepare(query string) (driver.Stmt, error) {
	plan := &user.CreatePreparedStatementReq{SqlQuery: &query}
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_CREATE_PREPARED_STATEMENT, d.nextCoordID(), plan)
	if err != nil {
		return nil, err
	}

	d.conn.Write(makePrefixedMessage(encoded))

	resp := &user.CreatePreparedStatementResp{}
	if _, err = readPrefixedMessage(d.conn, resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("got error: %s", resp.GetError().GetMessage())
	}

	return &prepared{stmt: resp.PreparedStatement, client: d}, nil
}

func (d *drillClient) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (d *drillClient) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	fmt.Println(query, args)
	if len(args) > 0 {
		return nil, errors.New("drill driver does not support prepared statements")
	}

	qid, err := d.SubmitQuery(shared.QueryType_SQL, query)
	if err != nil {
		log.Println(err)
		return nil, driver.ErrBadConn
	}

	r := &rows{
		batches: make(chan *rowbatch, 10),
	}
	log.Println(*qid)
	go d.getQueryResults(ctx, qid, r.batches)

	return r, nil
}

func (d *drillClient) Ping(context.Context) error {
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_PING, user.RpcType_ACK, d.nextCoordID(), &rpc.Ack{Ok: proto.Bool(true)})
	if err != nil {
		return driver.ErrBadConn
	}

	_, err = d.conn.Write(makePrefixedMessage(encoded))
	if err != nil {
		log.Println(err)
		return driver.ErrBadConn
	}

	msg, err := readPrefixedRaw(d.conn)
	if err != nil {
		log.Println(err)
		return driver.ErrBadConn
	}

	if msg.Header.GetMode() != rpc.RpcMode_PONG {
		return driver.ErrBadConn
	}

	return nil
}

func (d *drillClient) ExecuteStmt(stmt *user.PreparedStatement) (*shared.QueryId, error) {
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_RUN_QUERY, d.nextCoordID(), &user.RunQuery{
		ResultsMode:             user.QueryResultsMode_STREAM_FULL.Enum(),
		Type:                    shared.QueryType_PREPARED_STATEMENT.Enum(),
		PreparedStatementHandle: stmt.ServerHandle,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if _, err = d.conn.Write(makePrefixedMessage(encoded)); err != nil {
		log.Println(err)
		return nil, err
	}

	resp := &shared.QueryId{}
	_, err = readPrefixedMessage(d.conn, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
