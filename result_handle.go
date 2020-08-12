package drill

import (
	"errors"
	"fmt"

	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
)

type ResultHandle struct {
	dataChannel chan *queryData
	queryResult *shared.QueryResult
	curBatch    *RecordBatch

	queryID *shared.QueryId
	client  *drillClient
}

type RecordBatch struct {
	Def  *shared.RecordBatchDef
	Vecs []data.DataVector
}

func (r *ResultHandle) Close() error {
	close(r.dataChannel)
	r.client.resultMap.Delete(qid{*r.queryID.Part1, *r.queryID.Part2})
	return nil
}

func (r *ResultHandle) GetRecordBatch() *RecordBatch {
	if r.curBatch == nil {
		r.nextBatch()
	}

	return r.curBatch
}

func (r *ResultHandle) GetCols() []string {
	if r.curBatch == nil {
		r.nextBatch()
	}

	cols := make([]string, len(r.curBatch.Def.GetField()))
	for idx, f := range r.curBatch.Def.GetField() {
		cols[idx] = *f.NamePart.Name
	}

	return cols
}

func (r *ResultHandle) Cancel() {
	r.client.sendCancel(r.queryID)
}

var (
	QueryCompleted    = errors.New("completed")
	QueryCancelled    = errors.New("cancelled")
	QueryUnknownState = errors.New("unknown state")
)

func (r *ResultHandle) Next() error {
	r.curBatch = nil
	r.nextBatch()
	if r.curBatch != nil {
		return nil
	}

	if r.queryResult == nil {
		return QueryUnknownState
	}

	switch r.queryResult.GetQueryState() {
	case shared.QueryResult_COMPLETED:
		return QueryCompleted
	case shared.QueryResult_CANCELED:
		return QueryCancelled
	case shared.QueryResult_FAILED:
		return fmt.Errorf("query failed with error: %s", r.queryResult.GetError()[0].GetMessage())
	default:
		return QueryUnknownState
	}
}

func (r *ResultHandle) nextBatch() {
	q, ok := <-r.dataChannel
	if !ok {
		return
	}

	switch q.typ {
	case int32(user.RpcType_QUERY_DATA):
		qd := q.msg.(*shared.QueryData)
		r.curBatch = &RecordBatch{
			Vecs: make([]data.DataVector, 0, len(qd.GetDef().GetField())),
			Def:  qd.GetDef(),
		}

		var offset int32 = 0
		for _, f := range qd.GetDef().GetField() {
			r.curBatch.Vecs = append(r.curBatch.Vecs, data.NewValueVec(q.raw[offset:offset+f.GetBufferLength()], f))
			offset += f.GetBufferLength()
		}
	case int32(user.RpcType_QUERY_RESULT):
		r.queryResult = q.msg.(*shared.QueryResult)
	}
}
