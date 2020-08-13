package drill

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
)

//go:generate go run ./internal/cmd/drillProto runall ../../../

type qid struct {
	part1 int64
	part2 int64
}

type queryData struct {
	typ int32
	msg proto.Message
	raw []byte
}

// A Drillbit represents a single endpoint in the cluster
type Drillbit interface {
	GetAddress() string
	GetUserPort() int32
}

// A Conn represents a single connection to a drill bit. This interface
// is useful for things consuming the Client to maintain a separation
// so that it is easy to mock out for testing.
type Conn interface {
	ConnectEndpoint(context.Context, Drillbit) error
	Connect(context.Context) error
	ConnectWithZK(context.Context, ...string) error
	Ping(context.Context) error
	SubmitQuery(shared.QueryType, string) (*ResultHandle, error)
	PrepareQuery(string) (PreparedHandle, error)
	ExecuteStmt(PreparedHandle) (*ResultHandle, error)
	NewConnection(context.Context) (Conn, error)
	Close() error
}

// A Client is used for communicating to a drill cluster.
//
// After creating a client via one of the NewDrillClient functions, one
// of the Connect functions can be called to actually connect to the cluster.
type Client struct {
	// Modifying the options after connecting does not affect the current connection
	// it will only affect future connections of this client.
	Opts Options

	conn      net.Conn
	drillBits []string
	nextBit   int
	coordID   int32
	endpoint  Drillbit
	zkNodes   []string

	serverInfo      *user.BitToUserHandshake
	cancelHeartBeat context.CancelFunc

	resultMap sync.Map
	queryMap  sync.Map
	pingpong  chan bool
	outbound  chan []byte
	close     chan struct{}
}

// NewDrillClient initializes a Drill Client with the given options but does not
// actually connect yet. It also allows specifying the zookeeper cluster nodes here.
func NewDrillClient(opts Options, zk ...string) *Client {
	return &Client{
		close:    make(chan struct{}),
		outbound: make(chan []byte, 10),
		pingpong: make(chan bool),
		coordID:  int32(rand.Int()%1729 + 1),
		zkNodes:  zk,
		Opts:     opts,
	}
}

// NewConnection will use the stored zookeeper quorum nodes and drill bit information
// to find the next drill bit to connect to in order to spread out the load.
//
// The client returned from this will already be connected using the same options
// and zookeeper cluster as the current Client, just picking a different endpoint
// to connect to.
func (d *Client) NewConnection(ctx context.Context) (Conn, error) {
	c := NewDrillClient(d.Opts, d.zkNodes...)

	if len(d.drillBits) == 0 {
		err := c.Connect(ctx)
		d.drillBits = c.drillBits
		d.nextBit = c.nextBit
		return c, err
	}

	c.drillBits = d.drillBits
	eindex := d.nextBit
	d.nextBit++
	if d.nextBit >= len(c.drillBits) {
		d.nextBit = 0
	}

	c.nextBit = d.nextBit

	zook, err := newZKHandler(c.Opts.ClusterName, c.zkNodes...)
	if err != nil {
		return nil, err
	}
	defer zook.Close()

	return c, c.ConnectEndpoint(ctx, zook.GetEndpoint(c.drillBits[eindex]))
}

func (d *Client) recvRoutine() {
	type readData struct {
		msg *rpc.CompleteRpcMessage
		err error
	}

	inbound := make(chan readData)
	go func() {
		for {
			msg, err := readPrefixedRaw(d.conn)
			if err != nil {
				inbound <- readData{err: err}
				break
			}
			inbound <- readData{msg: msg}
		}
	}()

	defer func() {
		close(d.pingpong)
		d.queryMap.Range(func(_, val interface{}) bool {
			close(val.(chan *rpc.CompleteRpcMessage))
			return true
		})
		d.resultMap.Range(func(_, val interface{}) bool {
			close(val.(chan *queryData))
			return true
		})
	}()

	if d.Opts.HeartbeatFreq == nil {
		d.Opts.HeartbeatFreq = new(time.Duration)
		*d.Opts.HeartbeatFreq = defaultHeartbeatFreq
	}

	if d.Opts.HeartbeatFreq != nil && d.Opts.HeartbeatFreq.Seconds() > 0 {
		var heartBeatCtx context.Context
		heartBeatCtx, d.cancelHeartBeat = context.WithCancel(context.Background())
		go func() {
			ticker := time.NewTicker(*d.Opts.HeartbeatFreq)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if d.Ping(heartBeatCtx) != nil {
						return
					}
				case <-heartBeatCtx.Done():
					return
				}
			}
		}()
	}

	for {
		select {
		case <-d.close:
			return
		case encoded := <-d.outbound:
			_, err := d.conn.Write(makePrefixedMessage(encoded))
			if err != nil {
				// do something
			}
		case data := <-inbound:
			if data.err != nil {
				d.Close()
				return
			}

			if data.msg.Header.GetMode() == rpc.RpcMode_PONG {
				d.pingpong <- true
			}

			switch data.msg.GetHeader().GetRpcType() {
			case int32(user.RpcType_ACK):
				continue
			case int32(user.RpcType_QUERY_HANDLE):
				c, ok := d.queryMap.Load(data.msg.Header.GetCoordinationId())
				if !ok {
					// do something with error
				}

				c.(chan *rpc.CompleteRpcMessage) <- data.msg
			case int32(user.RpcType_QUERY_DATA):
				d.passQueryResponse(data.msg, &shared.QueryData{})
			case int32(user.RpcType_QUERY_RESULT):
				d.passQueryResponse(data.msg, &shared.QueryResult{})
			}
		}
	}
}

func (d *Client) passQueryResponse(data *rpc.CompleteRpcMessage, msg proto.Message) {
	d.sendAck(data.Header.GetCoordinationId(), true)
	if err := proto.Unmarshal(data.ProtobufBody, msg); err != nil {
		// do something
	}

	qidField := msg.ProtoReflect().Descriptor().Fields().ByName("query_id")
	q := msg.ProtoReflect().Get(qidField).Message().Interface().(*shared.QueryId)

	c, _ := d.resultMap.Load(qid{q.GetPart1(), q.GetPart2()})
	c.(chan *queryData) <- &queryData{data.Header.GetRpcType(), msg, data.GetRawBody()}
}

func (d *Client) nextCoordID() (next int32) {
	next = d.coordID
	d.coordID++
	return
}

func (d *Client) sendCancel(qid *shared.QueryId) error {
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_CANCEL_QUERY, d.nextCoordID(), qid)
	if err != nil {
		return err
	}

	d.outbound <- encoded
	return nil
}

func (d *Client) sendAck(coordID int32, isOk bool) {
	ack := &rpc.Ack{
		Ok: &isOk,
	}

	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_RESPONSE, user.RpcType_ACK, coordID, ack)
	if err != nil {
		panic(err)
	}

	d.outbound <- encoded
}

// Close the connection and cleanup the background goroutines
func (d *Client) Close() error {
	if d.close != nil {
		close(d.close)
		d.close = nil
	}
	if d.cancelHeartBeat != nil {
		d.cancelHeartBeat()
		d.cancelHeartBeat = nil
	}
	d.conn.Close()
	return nil
}

// ConnectEndpoint connects to the provided endpoint directly rather than looking for
// drillbits via zookeeper. This is also used by the normal connect setup to connect
// to the desired drillbit once it has been chosen from the zookeeper information.
//
// The provided context object will be passed to DialContext to control the deadlines
// for the socket connection, it will not be saved into the client.
func (d *Client) ConnectEndpoint(ctx context.Context, e Drillbit) error {
	d.endpoint = e
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.endpoint.GetAddress()+":"+strconv.Itoa(int(d.endpoint.GetUserPort())))
	if err != nil {
		return err
	}
	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetNoDelay(true)

	d.conn = conn
	if err = d.doHandshake(); err != nil {
		return err
	}

	go d.recvRoutine()

	return nil
}

// Connect attempts to use the current ZooKeeper cluster in order to find a drill bit
// to connect to. This will also populate the internal listing of drill bits from zookeeper.
//
// As with ConnectEndpoint, the context provided will be passed to DialContext
// and will not be stored in the client.
func (d *Client) Connect(ctx context.Context) error {
	if len(d.zkNodes) == 0 {
		return errors.New("no zookeeper nodes specified")
	}

	zoo, err := newZKHandler(d.Opts.ClusterName, d.zkNodes...)
	if err != nil {
		return err
	}
	defer zoo.Close()

	d.drillBits = zoo.GetDrillBits()
	rand.Shuffle(len(d.drillBits), func(i, j int) {
		d.drillBits[i], d.drillBits[j] = d.drillBits[j], d.drillBits[i]
	})

	d.nextBit = 1
	return d.ConnectEndpoint(ctx, zoo.GetEndpoint(d.drillBits[0]))
}

// ConnectWithZK overrides the current stored zookeeper cluster in the client, and
// uses the passed list of nodes to find a drillbit. This will replace the stored
// zookeeper nodes in the client with the new set provided.
//
// As with ConnectEndpoint, the context provided will be passed to DialContext
// and will not be stored in the client.
func (d *Client) ConnectWithZK(ctx context.Context, zkNode ...string) error {
	d.zkNodes = zkNode
	return d.Connect(ctx)
}

func (d *Client) MakeMetaRequest() *user.ServerMeta {
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_GET_SERVER_META, d.nextCoordID(), &user.GetServerMetaReq{})
	if err != nil {
		panic(err)
	}

	data := makePrefixedMessage(encoded)
	d.conn.Write(data)

	resp := &user.GetServerMetaResp{}
	hdr, err := readPrefixedMessage(d.conn, resp)
	fmt.Println(hdr)
	fmt.Println(resp.GetStatus())
	fmt.Println(resp.GetError())
	return resp.ServerMeta
}

// PrepareQuery creates a prepared sql statement and returns a handle to it. This
// handle can be used with any client connected to the same cluster in order to
// actually execute it with ExecuteStmt.
func (d *Client) PrepareQuery(plan string) (PreparedHandle, error) {
	coord := d.nextCoordID()
	req := &user.CreatePreparedStatementReq{SqlQuery: &plan}
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_CREATE_PREPARED_STATEMENT, coord, req)
	if err != nil {
		return nil, err
	}

	queryHandle := make(chan *rpc.CompleteRpcMessage)
	d.queryMap.Store(coord, queryHandle)

	d.outbound <- encoded
	rsp, ok := <-queryHandle
	if !ok || rsp == nil {
		return nil, errors.New("failed to read")
	}

	close(queryHandle)
	d.queryMap.Delete(coord)

	resp := &user.CreatePreparedStatementResp{}
	if err = proto.Unmarshal(rsp.GetProtobufBody(), resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("got error: %s", resp.GetError().GetMessage())
	}

	return resp.PreparedStatement, nil
}

// SubmitQuery submits the specified query and query type returning a handle to the results
// This only blocks long enough to receive a Query ID from the cluster, it does not
// wait for the results to start coming in. The result handle can be used to do that.
//
// If the query fails, this will not error but rather you'd retrieve that failure from
// the result handle itself.
func (d *Client) SubmitQuery(t shared.QueryType, plan string) (*ResultHandle, error) {
	query := &user.RunQuery{
		ResultsMode: user.QueryResultsMode_STREAM_FULL.Enum(),
		Type:        &t,
		Plan:        &plan,
	}

	coord := d.nextCoordID()
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_RUN_QUERY, coord, query)
	if err != nil {
		return nil, err
	}

	queryHandle := make(chan *rpc.CompleteRpcMessage)
	d.queryMap.Store(coord, queryHandle)

	d.outbound <- encoded
	rsp, ok := <-queryHandle
	if !ok || rsp == nil {
		return nil, errors.New("failed to read")
	}

	close(queryHandle)
	d.queryMap.Delete(coord)

	resp := &shared.QueryId{}
	err = proto.Unmarshal(rsp.ProtobufBody, resp)

	if err != nil {
		return nil, err
	}

	dataChannel := make(chan *queryData, 5)
	d.resultMap.Store(qid{resp.GetPart1(), resp.GetPart2()}, dataChannel)

	return &ResultHandle{
		dataChannel: dataChannel,
		queryID:     resp,
		client:      d,
	}, nil
}

// ExecuteStmt runs the passed prepared statement against the cluster and returns
// a handle to the results in the same way that SubmitQuery does.
func (d *Client) ExecuteStmt(hndl PreparedHandle) (*ResultHandle, error) {
	prep := hndl.(*user.PreparedStatement)
	if prep == nil {
		return nil, errors.New("invalid prepared statement handle")
	}

	coord := d.nextCoordID()
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_RUN_QUERY, coord, &user.RunQuery{
		ResultsMode:             user.QueryResultsMode_STREAM_FULL.Enum(),
		Type:                    shared.QueryType_PREPARED_STATEMENT.Enum(),
		PreparedStatementHandle: prep.ServerHandle,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	queryHandle := make(chan *rpc.CompleteRpcMessage)
	d.queryMap.Store(coord, queryHandle)
	d.outbound <- encoded
	rsp, ok := <-queryHandle
	if !ok || rsp == nil {
		return nil, errors.New("failed to read")
	}

	close(queryHandle)
	d.queryMap.Delete(coord)

	resp := &shared.QueryId{}
	err = proto.Unmarshal(rsp.GetProtobufBody(), resp)
	if err != nil {
		return nil, err
	}

	dataChannel := make(chan *queryData, 5)
	d.resultMap.Store(qid{resp.GetPart1(), resp.GetPart2()}, dataChannel)

	return &ResultHandle{
		dataChannel: dataChannel,
		queryID:     resp,
		client:      d,
	}, nil
}

// Ping sends a ping to the server via this connection and waits for a Pong response.
// Returns database/sql/driver.ErrBadConn if it fails or nil if it succeeds.
func (d *Client) Ping(ctx context.Context) error {
	coord := d.nextCoordID()
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_PING, user.RpcType_ACK, coord, &rpc.Ack{Ok: proto.Bool(true)})
	if err != nil {
		return driver.ErrBadConn
	}

	d.outbound <- encoded
	select {
	case val, ok := <-d.pingpong:
		if !ok || !val {
			return driver.ErrBadConn
		}
	case <-ctx.Done():
		return driver.ErrBadConn
	}

	return nil
}
