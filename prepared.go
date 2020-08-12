package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
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

	qid, err := p.client.ExecuteStmt(p.stmt)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			p.client.sendCancel(qid)
		case <-done:
		}
	}()

	var affectedRows int64 = 0

	for {
		msg, err := readPrefixedRaw(p.client.conn)
		if err != nil {
			return nil, err
		}

		switch msg.GetHeader().GetRpcType() {
		case int32(user.RpcType_QUERY_DATA):
			p.client.sendAck(msg.Header.GetCoordinationId(), true)

			qd := &shared.QueryData{}
			if err = proto.Unmarshal(msg.ProtobufBody, qd); err != nil {
				return nil, err
			}

			// qid should equal qd.QueryId
			affectedRows += int64(qd.GetAffectedRowsCount())
		case int32(user.RpcType_QUERY_RESULT):
			p.client.sendAck(*msg.Header.CoordinationId, true)

			qr := &shared.QueryResult{}
			if err = proto.Unmarshal(msg.ProtobufBody, qr); err != nil {
				return nil, err
			}

			switch qr.GetQueryState() {
			case shared.QueryResult_COMPLETED:
				return &result{rowsAffected: affectedRows, rowsError: nil}, nil
			case shared.QueryResult_CANCELED:
				return driver.ResultNoRows, errors.New("query cancelled")
			case shared.QueryResult_FAILED:
				if len(qr.GetError()) > 0 {
					return driver.ResultNoRows, fmt.Errorf("query failed with error: %s", qr.GetError()[0].GetMessage())
				}

				return driver.ResultNoRows, errors.New("query failed with unknown error")
			}
		}
	}
}

func (p *prepared) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("drill does not support parameters in prepared statements")
	}

	qid, err := p.client.ExecuteStmt(p.stmt)
	if err != nil {
		return nil, err
	}

	r := &rows{
		batches: make(chan *rowbatch, 10),
	}

	go p.client.getQueryResults(ctx, qid, r.batches)
	return r, nil
}
