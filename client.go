package drill

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/factset/go-drill/internal/log"
	"github.com/factset/go-drill/internal/rpc/proto/exec"
	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
)

//go:generate go run ./internal/cmd/drillProto runall ../../../

type QueryType shared.QueryType

const (
	TypeSQL      QueryType = QueryType(shared.QueryType_SQL)
	TypeLogical            = QueryType(shared.QueryType_LOGICAL)
	TypePhysical           = QueryType(shared.QueryType_PHYSICAL)
)

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
	GetEndpoint() Drillbit
	Ping(context.Context) error
	SubmitQuery(QueryType, string) (DataHandler, error)
	PrepareQuery(string) (PreparedHandle, error)
	ExecuteStmt(PreparedHandle) (DataHandler, error)
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
	connected bool

	drillBits []string
	nextBit   int
	coordID   int32
	endpoint  Drillbit
	ZkNodes   []string

	dataEncoder     encoder
	serverInfo      *user.BitToUserHandshake
	cancelHeartBeat context.CancelFunc
	hbMutex         sync.Mutex
	dataImpl        dataImplType

	resultMap sync.Map
	queryMap  sync.Map
	pingpong  chan bool
	outbound  chan []byte
	close     chan struct{}
}

// NewClient initializes a Drill Client with the given options but does not
// actually connect yet. It also allows specifying the zookeeper cluster nodes here.
func NewClient(opts Options, zk ...string) *Client {
	impl := basicData
	if opts.UseArrow {
		impl = arrowData
	}
	return &Client{
		close:       make(chan struct{}),
		outbound:    make(chan []byte, 10),
		pingpong:    make(chan bool),
		coordID:     int32(rand.Int()%1729 + 1),
		ZkNodes:     zk,
		dataEncoder: rpcEncoder{},
		Opts:        opts,
		dataImpl:    impl,
	}
}

// NewDirectClient initializes a Drill Client which connects to an endpoint directly
// rather than relying on ZooKeeper for finding drill bits.
func NewDirectClient(opts Options, host string, port int32) *Client {
	cl := NewClient(opts)
	cl.endpoint = &exec.DrillbitEndpoint{
		Address: proto.String(host), UserPort: proto.Int32(port),
	}
	return cl
}

var createZKHandler = newZKHandler

// GetEndpoint returns the currently configured endpoint that the client either
// is connected to or will connect to if Connect is called (in the case of a Direct
// connection client). Returns nil for a client using Zookeeper that hasn't connected
// yet, as the endpoint is only determined when connecting in that case.
func (d *Client) GetEndpoint() Drillbit {
	return d.endpoint
}

// NewConnection will use the stored zookeeper quorum nodes and drill bit information
// to find the next drill bit to connect to in order to spread out the load.
//
// The client returned from this will already be connected using the same options
// and zookeeper cluster as the current Client, just picking a different endpoint
// to connect to.
func (d *Client) NewConnection(ctx context.Context) (Conn, error) {
	newClient := NewClient(d.Opts, d.ZkNodes...)
	if len(newClient.ZkNodes) == 0 && d.endpoint != nil {
		newClient.endpoint = d.endpoint
	}

	if len(d.drillBits) == 0 {
		err := newClient.Connect(ctx)
		d.drillBits = newClient.drillBits
		d.nextBit = newClient.nextBit
		return newClient, err
	}

	newClient.drillBits = d.drillBits
	eindex := d.nextBit
	d.nextBit++
	if d.nextBit >= len(newClient.drillBits) {
		d.nextBit = 0
	}

	newClient.nextBit = d.nextBit

	if newClient.Opts.ZKPath == "" {
		newClient.Opts.ZKPath = "/drill/" + newClient.Opts.ClusterName
	}

	zook, err := createZKHandler(newClient.Opts.ClusterName, newClient.ZkNodes...)
	if err != nil {
		return nil, err
	}
	defer zook.Close()

	return newClient, newClient.ConnectEndpoint(ctx, zook.GetEndpoint(newClient.drillBits[eindex]))
}

func (d *Client) recvRoutine() {
	type readData struct {
		msg *rpc.CompleteRpcMessage
		err error
	}

	d.hbMutex.Lock()
	d.connected = true
	d.hbMutex.Unlock()

	inbound := make(chan readData)
	go func() {
		for {
			msg, err := d.dataEncoder.ReadRaw(d.conn)
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
		d.Close()
		d.conn.Close()
	}()

	if d.Opts.HeartbeatFreq == nil {
		d.Opts.HeartbeatFreq = new(time.Duration)
		*d.Opts.HeartbeatFreq = defaultHeartbeatFreq
	}

	if d.Opts.HeartbeatFreq != nil && d.Opts.HeartbeatFreq.Seconds() > 0 {
		var heartBeatCtx context.Context
		d.hbMutex.Lock()
		heartBeatCtx, d.cancelHeartBeat = context.WithCancel(context.Background())
		d.hbMutex.Unlock()

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
		case encoded, ok := <-d.outbound:
			if !ok {
				return
			}

			_, err := d.dataEncoder.WriteRaw(d.conn, encoded)
			if err != nil {
				return
			}
		case data := <-inbound:
			if data.err != nil {
				log.Print("drill: read error: ", data.err)
				return
			}

			if data.msg.Header.GetMode() == rpc.RpcMode_PONG {
				d.pingpong <- true
			}

			switch data.msg.GetHeader().GetRpcType() {
			case int32(user.RpcType_ACK):
				continue
			case int32(user.RpcType_SERVER_META),
				int32(user.RpcType_SCHEMAS),
				int32(user.RpcType_CATALOGS),
				int32(user.RpcType_TABLES),
				int32(user.RpcType_COLUMNS),
				int32(user.RpcType_QUERY_HANDLE):
				c, ok := d.queryMap.Load(data.msg.Header.GetCoordinationId())
				if !ok || c == nil {
					log.Print("couldn't find query channel for response")
					continue
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
		log.Print("couldn't unmarshal query data from response")
		return
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
	encoded, err := encodeRPCMessage(rpc.RpcMode_REQUEST, user.RpcType_CANCEL_QUERY, d.nextCoordID(), qid)
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

	encoded, err := encodeRPCMessage(rpc.RpcMode_RESPONSE, user.RpcType_ACK, coordID, ack)
	if err != nil {
		panic(err)
	}

	d.outbound <- encoded
}

// Close the connection and cleanup the background goroutines
func (d *Client) Close() error {
	d.hbMutex.Lock()
	defer d.hbMutex.Unlock()
	if d.connected {
		close(d.close)
	}
	if d.cancelHeartBeat != nil {
		d.cancelHeartBeat()
		d.cancelHeartBeat = nil
	}
	d.connected = false
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
	if len(d.ZkNodes) == 0 {
		if d.endpoint != nil {
			return d.ConnectEndpoint(ctx, d.endpoint)
		}

		return errors.New("no zookeeper nodes specified")
	}

	if d.Opts.ZKPath == "" {
		d.Opts.ZKPath = "/drill/" + d.Opts.ClusterName
	}

	zoo, err := createZKHandler(d.Opts.ZKPath, d.ZkNodes...)
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
	d.ZkNodes = zkNode
	return d.Connect(ctx)
}

func (d *Client) makeReqGetResp(mode rpc.RpcMode, msgType user.RpcType, req, resp proto.Message) error {
	coord := d.nextCoordID()
	encoded, err := encodeRPCMessage(mode, msgType, coord, req)
	if err != nil {
		return err
	}

	queryHandle := make(chan *rpc.CompleteRpcMessage)
	d.queryMap.Store(coord, queryHandle)
	defer d.queryMap.Delete(coord)

	d.outbound <- encoded
	rsp, ok := <-queryHandle
	if !ok || rsp == nil {
		return errors.New("failed to read")
	}
	close(queryHandle)

	return proto.Unmarshal(rsp.GetProtobufBody(), resp)
}

// PrepareQuery creates a prepared sql statement and returns a handle to it. This
// handle can be used with any client connected to the same cluster in order to
// actually execute it with ExecuteStmt.
func (d *Client) PrepareQuery(plan string) (PreparedHandle, error) {
	req := &user.CreatePreparedStatementReq{SqlQuery: &plan}

	resp := &user.CreatePreparedStatementResp{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_CREATE_PREPARED_STATEMENT, req, resp); err != nil {
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
func (d *Client) SubmitQuery(t QueryType, plan string) (DataHandler, error) {
	qt := shared.QueryType(t)
	query := &user.RunQuery{
		ResultsMode: user.QueryResultsMode_STREAM_FULL.Enum(),
		Type:        &qt,
		Plan:        &plan,
	}

	resp := &shared.QueryId{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_RUN_QUERY, query, resp); err != nil {
		return nil, err
	}

	dataChannel := make(chan *queryData, 5)
	d.resultMap.Store(qid{resp.GetPart1(), resp.GetPart2()}, dataChannel)

	return &ResultHandle{
		dataChannel: dataChannel,
		queryID:     resp,
		client:      d,
		implType:    d.dataImpl,
	}, nil
}

// ExecuteStmt runs the passed prepared statement against the cluster and returns
// a handle to the results in the same way that SubmitQuery does.
func (d *Client) ExecuteStmt(hndl PreparedHandle) (DataHandler, error) {
	prep := hndl.(*user.PreparedStatement)
	if prep == nil {
		return nil, errors.New("invalid prepared statement handle")
	}

	req := &user.RunQuery{
		ResultsMode:             user.QueryResultsMode_STREAM_FULL.Enum(),
		Type:                    shared.QueryType_PREPARED_STATEMENT.Enum(),
		PreparedStatementHandle: prep.ServerHandle,
	}

	resp := &shared.QueryId{}
	if err := d.makeReqGetResp(rpc.RpcMode_REQUEST, user.RpcType_RUN_QUERY, req, resp); err != nil {
		return nil, err
	}

	dataChannel := make(chan *queryData, 5)
	d.resultMap.Store(qid{resp.GetPart1(), resp.GetPart2()}, dataChannel)

	return &ResultHandle{
		dataChannel: dataChannel,
		queryID:     resp,
		client:      d,
		implType:    d.dataImpl,
	}, nil
}

// Ping sends a ping to the server via this connection and waits for a Pong response.
// Returns database/sql/driver.ErrBadConn if it fails or nil if it succeeds.
func (d *Client) Ping(ctx context.Context) error {
	coord := d.nextCoordID()
	encoded, err := encodeRPCMessage(rpc.RpcMode_PING, user.RpcType_ACK, coord, &rpc.Ack{Ok: proto.Bool(true)})
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
