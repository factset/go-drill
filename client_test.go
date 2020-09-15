package drill

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
)

func TestNewClient(t *testing.T) {
	heartbeat := 5 * time.Second
	opts := Options{
		Schema:        "foobar",
		SaslEncrypt:   true,
		HeartbeatFreq: &heartbeat,
	}

	cl := NewClient(opts, "zknode1", "zknode2", "zknode3")

	assert.NotNil(t, cl.close)
	assert.NotNil(t, cl.outbound)
	assert.NotNil(t, cl.pingpong)
	assert.ElementsMatch(t, []string{"zknode1", "zknode2", "zknode3"}, cl.ZkNodes)
	assert.IsType(t, rpcEncoder{}, cl.dataEncoder)
	assert.Equal(t, opts, cl.Opts)
	assert.GreaterOrEqual(t, cl.coordID, int32(0))
}

func TestNewDirectClient(t *testing.T) {
	heartbeat := 5 * time.Second
	opts := Options{
		Schema:        "foobar",
		SaslEncrypt:   true,
		HeartbeatFreq: &heartbeat,
	}

	cl := NewDirectClient(opts, "localhost", 1234)

	assert.NotNil(t, cl.close)
	assert.NotNil(t, cl.outbound)
	assert.NotNil(t, cl.pingpong)
	assert.IsType(t, rpcEncoder{}, cl.dataEncoder)
	assert.Equal(t, opts, cl.Opts)
	assert.GreaterOrEqual(t, cl.coordID, int32(0))
	assert.Equal(t, "localhost", cl.endpoint.GetAddress())
	assert.Equal(t, int32(1234), cl.endpoint.GetUserPort())
}

type TCPServer struct {
	port   int32
	server net.Listener

	handler func(net.Conn)
}

func (t *TCPServer) Close() (err error) {
	return t.server.Close()
}

func (t *TCPServer) Run() (err error) {
	t.server, err = net.Listen("tcp", ":0")
	if err != nil {
		return
	}

	addr, _ := net.ResolveTCPAddr("tcp", t.server.Addr().String())
	t.port = int32(addr.Port)

	go func() {
		for {
			conn, err := t.server.Accept()
			if err != nil || conn == nil {
				err = errors.New("could not accept connection")
				break
			}

			go t.handler(conn)
		}
	}()
	return
}

func mockHandshake(handShake *user.HandshakeStatus) func(net.Conn) {
	status := handShake
	if handShake == nil {
		status = user.HandshakeStatus_SUCCESS.Enum()
	}

	enc := rpcEncoder{}

	return func(conn net.Conn) {
		defer conn.Close()

		for {
			msg, err := enc.ReadRaw(conn)
			if err != nil {
				return
			}

			switch msg.Header.GetRpcType() {
			case int32(user.RpcType_HANDSHAKE):
				enc.Write(conn, rpc.RpcMode_RESPONSE, user.RpcType_HANDSHAKE, 1, &user.BitToUserHandshake{
					Encrypted: proto.Bool(false),
					Status:    status,
				})
			}
		}
	}
}

func TestConnectEndpointFail(t *testing.T) {
	cl := NewClient(Options{})
	bit := &exec.DrillbitEndpoint{
		Address: proto.String(""),
	}
	assert.Error(t, cl.ConnectEndpoint(context.Background(), bit))
}

func TestConnectEndpoint(t *testing.T) {
	server := TCPServer{handler: mockHandshake(nil)}

	defer server.Close()
	server.Run()

	bit := &exec.DrillbitEndpoint{
		Address:  proto.String(""),
		UserPort: proto.Int32(server.port),
	}

	cl := NewClient(Options{})
	assert.NoError(t, cl.ConnectEndpoint(context.Background(), bit))
	cl.Close()
}

func TestConnectEndpointFailHandshake(t *testing.T) {
	server := TCPServer{handler: mockHandshake(user.HandshakeStatus_UNKNOWN_FAILURE.Enum())}

	defer server.Close()
	server.Run()

	bit := &exec.DrillbitEndpoint{
		Address:  proto.String(""),
		UserPort: proto.Int32(server.port),
	}

	cl := NewClient(Options{})
	assert.Error(t, cl.ConnectEndpoint(context.Background(), bit))
}

func TestClientConnectNoZK(t *testing.T) {
	cl := Client{}
	assert.EqualError(t, cl.Connect(context.Background()), "no zookeeper nodes specified")
}

func TestDirectClientConnect(t *testing.T) {
	server := TCPServer{handler: mockHandshake(nil)}

	defer server.Close()
	server.Run()

	cl := NewDirectClient(Options{}, "", server.port)
	assert.NoError(t, cl.Connect(context.Background()))
	cl.Close()
}

func TestClientConnectFailZK(t *testing.T) {
	cl := NewClient(Options{}, "zknode1")
	assert.Error(t, cl.Connect(context.Background()))
}

func TestClientConnectErr(t *testing.T) {
	defer func(orig func(string, ...string) (*zkHandler, error)) {
		createZKHandler = orig
	}(createZKHandler)

	mzk := new(mockzk)
	mzk.Test(t)
	defer mzk.AssertExpectations(t)

	mzk.On("Children", "/drill/drill").Return([]string{"bit1", "bit2", "bit3"}, (*zk.Stat)(nil), nil).Once()
	service := &exec.DrillServiceInstance{
		Id: proto.String("bit"),
		Endpoint: &exec.DrillbitEndpoint{
			Address:  proto.String("foobar"),
			UserPort: proto.Int32(2020),
		},
	}

	data, _ := proto.Marshal(service)
	mzk.On("Get", mock.Anything).Return(data, (*zk.Stat)(nil), nil).Once()
	mzk.On("Close").Once()

	createZKHandler = func(clst string, nodes ...string) (*zkHandler, error) {
		assert.Equal(t, "drill", clst)
		assert.ElementsMatch(t, []string{"a", "b", "c"}, nodes)
		return &zkHandler{conn: mzk, Path: "/drill/drill"}, nil
	}

	cl := NewClient(Options{ZKPath: "drill"}, "a", "b", "c")
	assert.Error(t, cl.Connect(context.Background()))
}

func TestClientConnect(t *testing.T) {
	defer func(orig func(string, ...string) (*zkHandler, error)) {
		createZKHandler = orig
	}(createZKHandler)

	mzk := new(mockzk)
	mzk.Test(t)
	defer mzk.AssertExpectations(t)

	mzk.On("Children", "/drill/drill").Return([]string{"bit1", "bit2", "bit3"}, (*zk.Stat)(nil), nil).Once()

	server := TCPServer{handler: mockHandshake(nil)}
	defer server.Close()

	server.Run()

	service := &exec.DrillServiceInstance{
		Id: proto.String("bit"),
		Endpoint: &exec.DrillbitEndpoint{
			Address:  proto.String(""),
			UserPort: proto.Int32(server.port),
		},
	}

	data, _ := proto.Marshal(service)
	mzk.On("Get", mock.Anything).Return(data, (*zk.Stat)(nil), nil).Once()
	mzk.On("Close").Once()

	createZKHandler = func(clst string, nodes ...string) (*zkHandler, error) {
		assert.Equal(t, "/drill/drill", clst)
		assert.ElementsMatch(t, []string{"a", "b", "c"}, nodes)
		return &zkHandler{conn: mzk, Path: "/drill/drill"}, nil
	}

	cl := NewClient(Options{ClusterName: "drill"}, "a", "b", "c")
	assert.NoError(t, cl.Connect(context.Background()))
}

func TestConnectWithZK(t *testing.T) {
	defer func(orig func(string, ...string) (*zkHandler, error)) {
		createZKHandler = orig
	}(createZKHandler)

	createZKHandler = func(clst string, nodes ...string) (*zkHandler, error) {
		assert.ElementsMatch(t, []string{"1", "2", "3"}, nodes)
		return nil, assert.AnError
	}

	cl := NewClient(Options{}, "a", "b", "c")
	assert.Same(t, assert.AnError, cl.ConnectWithZK(context.Background(), "1", "2", "3"))
}

func TestNewConnection(t *testing.T) {
	heartbeat := time.Second * 5
	opts := Options{
		Schema:        "foobar",
		SaslEncrypt:   true,
		HeartbeatFreq: &heartbeat,
	}

	defer func(orig func(string, ...string) (*zkHandler, error)) {
		createZKHandler = orig
	}(createZKHandler)

	mzk := new(mockzk)
	mzk.Test(t)
	defer mzk.AssertExpectations(t)

	mzk.On("Children", "/drill/drill").Return([]string{"bit1", "bit2", "bit3"}, (*zk.Stat)(nil), nil).Once()
	service := &exec.DrillServiceInstance{
		Id: proto.String("bit"),
		Endpoint: &exec.DrillbitEndpoint{
			Address:  proto.String("foobar"),
			UserPort: proto.Int32(2020),
		},
	}

	data, _ := proto.Marshal(service)
	mzk.On("Get", mock.Anything).Return(data, (*zk.Stat)(nil), nil).Once()
	mzk.On("Close").Once()

	createZKHandler = func(clst string, nodes ...string) (*zkHandler, error) {
		assert.ElementsMatch(t, []string{"a", "b", "c"}, nodes)
		return &zkHandler{conn: mzk, Path: "/drill/drill"}, nil
	}

	cl := NewClient(opts, "a", "b", "c")
	nc, err := cl.NewConnection(context.Background())

	cl.Opts.ZKPath = "/drill/"
	assert.Error(t, err)
	assert.Equal(t, cl.Opts, nc.(*Client).Opts)
	assert.EqualValues(t, cl.drillBits, nc.(*Client).drillBits)
	assert.EqualValues(t, cl.nextBit, nc.(*Client).nextBit)
}

func TestClientNewConnectionZKFail(t *testing.T) {
	defer func(orig func(string, ...string) (*zkHandler, error)) {
		createZKHandler = orig
	}(createZKHandler)

	createZKHandler = func(clst string, nodes ...string) (*zkHandler, error) {
		return nil, assert.AnError
	}

	cl := NewClient(Options{}, "a", "b", "c")
	cl.drillBits = []string{"bit1", "bit2", "bit3"}
	cl.nextBit = 1

	nc, err := cl.NewConnection(context.Background())
	assert.Same(t, assert.AnError, err)
	assert.Equal(t, 2, cl.nextBit)
	assert.Nil(t, nc)
}

func TestClientNewConnectionNextBit(t *testing.T) {
	heartbeat := time.Second * 5
	opts := Options{
		Schema:        "foobar",
		SaslEncrypt:   true,
		HeartbeatFreq: &heartbeat,
		ClusterName:   "cluster",
	}

	defer func(orig func(string, ...string) (*zkHandler, error)) {
		createZKHandler = orig
	}(createZKHandler)

	mzk := new(mockzk)
	mzk.Test(t)
	defer mzk.AssertExpectations(t)

	createZKHandler = func(clst string, nodes ...string) (*zkHandler, error) {
		assert.Equal(t, "cluster", clst)
		assert.ElementsMatch(t, []string{"a", "b", "c"}, nodes)
		return &zkHandler{conn: mzk, Path: "/drill/drill"}, nil
	}

	service := &exec.DrillServiceInstance{
		Id: proto.String("bit3"),
		Endpoint: &exec.DrillbitEndpoint{
			Address:  proto.String("foobar"),
			UserPort: proto.Int32(2020),
		},
	}

	data, _ := proto.Marshal(service)
	mzk.On("Get", "/drill/drill/bit3").Return(data, (*zk.Stat)(nil), nil).Once()
	mzk.On("Close").Once()

	cl := NewClient(opts, "a", "b", "c")
	cl.drillBits = []string{"bit1", "bit2", "bit3"}
	cl.nextBit = 2

	nc, err := cl.NewConnection(context.Background())
	assert.Error(t, err)

	assert.Equal(t, 0, cl.nextBit)
	assert.Equal(t, 0, nc.(*Client).nextBit)
	assert.Equal(t, cl.drillBits, nc.(*Client).drillBits)
}

func TestDirectClientNewConnection(t *testing.T) {
	server := TCPServer{handler: mockHandshake(nil)}

	defer server.Close()
	server.Run()

	cl := NewDirectClient(Options{}, "", server.port)
	nc, err := cl.NewConnection(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, nc)
	assert.Equal(t, server.port, nc.GetEndpoint().GetUserPort())
	assert.Equal(t, "", nc.GetEndpoint().GetAddress())
}

func TestClientSubmitQuery(t *testing.T) {
	cl := NewClient(Options{})

	cl.coordID = 0

	queryID := &shared.QueryId{Part1: proto.Int64(12345), Part2: proto.Int64(98765)}
	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.RunQuery{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_RUN_QUERY, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.EqualValues(t, user.QueryResultsMode_STREAM_FULL, msg.GetResultsMode())
		assert.EqualValues(t, shared.QueryType_SQL, msg.GetType())
		assert.Equal(t, "foobar", msg.GetPlan())

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(queryID)

		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	rh, err := cl.SubmitQuery(shared.QueryType_SQL, "foobar")
	assert.NoError(t, err)
	assert.Same(t, cl, rh.(*ResultHandle).client)
	assert.EqualValues(t, 1, cl.coordID)

	_, ok := cl.queryMap.Load(0)
	assert.False(t, ok)

	ch, ok := cl.resultMap.Load(qid{12345, 98765})
	assert.True(t, ok)
	assert.Exactly(t, ch, rh.(*ResultHandle).dataChannel)

	assert.Equal(t, queryID.GetPart1(), rh.(*ResultHandle).queryID.GetPart1())
	assert.Equal(t, queryID.GetPart2(), rh.(*ResultHandle).queryID.GetPart2())
}

func TestClientReqNilResp(t *testing.T) {
	cl := NewClient(Options{})

	tests := []struct {
		name string
		call func(*Client) (interface{}, error)
	}{
		{"submit query", func(cl *Client) (interface{}, error) { return cl.SubmitQuery(shared.QueryType_SQL, "foobar") }},
		{"prepare query", func(cl *Client) (interface{}, error) { return cl.PrepareQuery("foobar") }},
		{"execute stmt", func(cl *Client) (interface{}, error) { return cl.ExecuteStmt(&user.PreparedStatement{}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cl.coordID = 0
			go func() {
				<-cl.outbound
				ch, _ := cl.queryMap.Load(int32(0))
				ch.(chan *rpc.CompleteRpcMessage) <- nil
			}()

			val, err := tt.call(cl)
			assert.Nil(t, val)
			assert.EqualError(t, err, "failed to read")
			_, ok := cl.queryMap.Load(0)
			assert.False(t, ok)
		})
	}
}

func TestClientReqClosedChannel(t *testing.T) {
	cl := NewClient(Options{})

	tests := []struct {
		name string
		call func(*Client) (interface{}, error)
	}{
		{"submit query", func(cl *Client) (interface{}, error) { return cl.SubmitQuery(shared.QueryType_SQL, "foobar") }},
		{"prepare query", func(cl *Client) (interface{}, error) { return cl.PrepareQuery("foobar") }},
		{"execute stmt", func(cl *Client) (interface{}, error) { return cl.ExecuteStmt(&user.PreparedStatement{}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cl.coordID = 0
			go func() {
				<-cl.outbound
				ch, _ := cl.queryMap.Load(int32(0))
				close(ch.(chan *rpc.CompleteRpcMessage))
			}()

			val, err := tt.call(cl)
			assert.Nil(t, val)
			assert.EqualError(t, err, "failed to read")
			_, ok := cl.queryMap.Load(0)
			assert.False(t, ok)
		})
	}
}

func TestClientReqFailUnmarshal(t *testing.T) {
	cl := NewClient(Options{})

	tests := []struct {
		name string
		call func(*Client) (interface{}, error)
	}{
		{"submit query", func(cl *Client) (interface{}, error) { return cl.SubmitQuery(shared.QueryType_SQL, "foobar") }},
		{"prepare query", func(cl *Client) (interface{}, error) { return cl.PrepareQuery("foobar") }},
		{"execute stmt", func(cl *Client) (interface{}, error) { return cl.ExecuteStmt(&user.PreparedStatement{}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			cl.coordID = 0
			go func() {
				<-cl.outbound
				ch, _ := cl.queryMap.Load(int32(0))
				ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
					ProtobufBody: deadbeef,
				}
			}()

			val, err := tt.call(cl)
			assert.Nil(t, val)
			assert.Same(t, io.ErrUnexpectedEOF, err)
			_, ok := cl.queryMap.Load(0)
			assert.False(t, ok)
		})
	}
}

func TestClientPrepareQuery(t *testing.T) {
	cl := NewClient(Options{})
	cl.coordID = 0

	resp := &user.CreatePreparedStatementResp{
		Status: user.RequestStatus_OK.Enum(),
		PreparedStatement: &user.PreparedStatement{
			ServerHandle: &user.PreparedStatementHandle{
				ServerInfo: deadbeef,
			},
		},
	}

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.CreatePreparedStatementReq{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_CREATE_PREPARED_STATEMENT, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.Equal(t, "foobar", msg.GetSqlQuery())

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(resp)

		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	ph, err := cl.PrepareQuery("foobar")
	assert.NoError(t, err)
	assert.Equal(t, deadbeef, ph.(*user.PreparedStatement).ServerHandle.ServerInfo)

	_, ok := cl.queryMap.Load(0)
	assert.False(t, ok)
}

func TestClientPrepareQueryBadStatus(t *testing.T) {
	cl := NewClient(Options{})

	tests := []struct {
		name   string
		status *user.RequestStatus
	}{
		{"status failed", user.RequestStatus_FAILED.Enum()},
		{"timeout status", user.RequestStatus_TIMEOUT.Enum()},
		{"unknown status", user.RequestStatus_UNKNOWN_STATUS.Enum()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl.coordID = 0

			resp := &user.CreatePreparedStatementResp{
				Status: tt.status,
				Error: &shared.DrillPBError{
					Message: proto.String("superduper"),
				},
			}

			go func() {
				encoded := <-cl.outbound

				msg := &user.CreatePreparedStatementReq{}
				_, err := decodeRPCMessage(encoded, msg)
				require.NoError(t, err)

				ch, _ := cl.queryMap.Load(int32(0))
				body, _ := proto.Marshal(resp)

				ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
					ProtobufBody: body,
				}
			}()

			ph, err := cl.PrepareQuery("foobar")
			assert.Nil(t, ph)
			assert.EqualError(t, err, "got error: superduper")

			_, ok := cl.queryMap.Load(0)
			assert.False(t, ok)
		})
	}
}

func TestClientExecuteStmt(t *testing.T) {
	cl := NewClient(Options{})

	cl.coordID = 0

	queryID := &shared.QueryId{Part1: proto.Int64(12345), Part2: proto.Int64(98765)}
	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &user.RunQuery{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_REQUEST, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_RUN_QUERY, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.EqualValues(t, user.QueryResultsMode_STREAM_FULL, msg.GetResultsMode())
		assert.EqualValues(t, shared.QueryType_PREPARED_STATEMENT, msg.GetType())
		assert.Equal(t, deadbeef, msg.PreparedStatementHandle.GetServerInfo())

		ch, ok := cl.queryMap.Load(int32(0))
		assert.True(t, ok)

		body, _ := proto.Marshal(queryID)

		ch.(chan *rpc.CompleteRpcMessage) <- &rpc.CompleteRpcMessage{
			ProtobufBody: body,
		}
	}()

	rh, err := cl.ExecuteStmt(&user.PreparedStatement{
		ServerHandle: &user.PreparedStatementHandle{
			ServerInfo: deadbeef,
		},
	})

	assert.NoError(t, err)
	assert.Same(t, cl, rh.(*ResultHandle).client)
	assert.EqualValues(t, 1, cl.coordID)

	_, ok := cl.queryMap.Load(0)
	assert.False(t, ok)

	ch, ok := cl.resultMap.Load(qid{12345, 98765})
	assert.True(t, ok)
	assert.Exactly(t, ch, rh.(*ResultHandle).dataChannel)

	assert.Equal(t, queryID.GetPart1(), rh.(*ResultHandle).queryID.GetPart1())
	assert.Equal(t, queryID.GetPart2(), rh.(*ResultHandle).queryID.GetPart2())
}

func TestClientExecuteStatementNil(t *testing.T) {
	cl := NewClient(Options{})
	rh, err := cl.ExecuteStmt((*user.PreparedStatement)(nil))
	assert.Nil(t, rh)
	assert.EqualError(t, err, "invalid prepared statement handle")
}

func TestClientPing(t *testing.T) {
	cl := NewClient(Options{})

	cl.coordID = 0

	go func() {
		encoded := <-cl.outbound
		require.NotNil(t, encoded)

		msg := &rpc.Ack{}
		hdr, err := decodeRPCMessage(encoded, msg)
		require.NoError(t, err)

		assert.Equal(t, rpc.RpcMode_PING, hdr.GetMode())
		assert.EqualValues(t, user.RpcType_ACK, hdr.GetRpcType())
		assert.EqualValues(t, 0, hdr.GetCoordinationId())

		assert.True(t, msg.GetOk())
		cl.pingpong <- true
	}()

	assert.Nil(t, cl.Ping(context.Background()))
	assert.EqualValues(t, 1, cl.coordID)
}

func TestClientPingClose(t *testing.T) {
	cl := NewClient(Options{})

	go func() {
		<-cl.outbound
		close(cl.pingpong)
	}()

	assert.Same(t, driver.ErrBadConn, cl.Ping(context.Background()))
}

func TestClientPingAlreadyClosed(t *testing.T) {
	cl := NewClient(Options{})
	close(cl.pingpong)

	go func() {
		<-cl.outbound
	}()

	assert.Same(t, driver.ErrBadConn, cl.Ping(context.Background()))
}

func TestClientPingFalse(t *testing.T) {
	cl := NewClient(Options{})

	go func() {
		<-cl.outbound
		cl.pingpong <- false
	}()

	assert.Same(t, driver.ErrBadConn, cl.Ping(context.Background()))
}

func TestClientPingContextCancel(t *testing.T) {
	cl := NewClient(Options{})

	go func() {
		<-cl.outbound
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	assert.Same(t, driver.ErrBadConn, cl.Ping(ctx))
}

type ClientRecvRoutineSuite struct {
	suite.Suite
	server TCPServer

	cl       *Client
	pings    int
	acked    int
	stateMtx sync.Mutex
}

func (suite *ClientRecvRoutineSuite) SetupSuite() {
	suite.server.handler = func(conn net.Conn) {
		defer conn.Close()

		enc := rpcEncoder{}
		for {
			msg, err := enc.ReadRaw(conn)
			if err != nil {
				return
			}

			if msg.Header.GetMode() == rpc.RpcMode_PING {
				suite.stateMtx.Lock()
				suite.pings++
				suite.stateMtx.Unlock()

				enc.Write(conn, rpc.RpcMode_PONG, user.RpcType_ACK, 1, nil)
				continue
			} else if msg.Header.GetRpcType() == int32(user.RpcType_ACK) {
				suite.stateMtx.Lock()
				suite.acked = int(msg.Header.GetCoordinationId())
				suite.stateMtx.Unlock()

				continue
			}

			response := &rpc.CompleteRpcMessage{
				Header: &rpc.RpcHeader{
					CoordinationId: proto.Int32(1),
				},
			}

			switch string(msg.GetProtobufBody()) {
			case "handle":
				enc.Write(conn, rpc.RpcMode_RESPONSE, user.RpcType_QUERY_HANDLE, int32(1),
					&shared.QueryId{Part1: proto.Int64(123456), Part2: proto.Int64(98765)})
				continue
			case "data":
				pm := &shared.QueryData{
					RowCount: proto.Int32(5),
					QueryId:  &shared.QueryId{Part1: proto.Int64(123456), Part2: proto.Int64(98765)},
				}
				response.Header.RpcType = proto.Int32(int32(user.RpcType_QUERY_DATA))
				response.ProtobufBody, _ = proto.Marshal(pm)
				response.RawBody = deadbeef
			case "baddata":
				response.Header.RpcType = proto.Int32(int32(user.RpcType_QUERY_DATA))
				response.ProtobufBody = deadbeef
			case "badresult":
				response.Header.RpcType = proto.Int32(int32(user.RpcType_QUERY_RESULT))
				response.ProtobufBody = deadbeef
			case "result":
				pm := &shared.QueryResult{
					QueryId:    &shared.QueryId{Part1: proto.Int64(123456), Part2: proto.Int64(98765)},
					QueryState: shared.QueryResult_COMPLETED.Enum(),
				}
				response.Header.RpcType = proto.Int32(int32(user.RpcType_QUERY_RESULT))
				response.ProtobufBody, _ = proto.Marshal(pm)
			}

			encoded, _ := proto.Marshal(response)
			enc.WriteRaw(conn, encoded)
		}
	}
}

func (suite *ClientRecvRoutineSuite) BeforeTest(_, testName string) {
	if strings.HasSuffix(testName, "WithHeartBeat") {
		freq := 100 * time.Millisecond
		suite.cl.Opts.HeartbeatFreq = &freq
	}

	go suite.cl.recvRoutine()
}

func (suite *ClientRecvRoutineSuite) SetupTest() {
	suite.stateMtx.Lock()
	defer suite.stateMtx.Unlock()

	suite.pings = 0
	suite.acked = -1

	suite.server.Run()
	suite.cl = NewClient(Options{})
	var err error
	suite.cl.conn, err = net.Dial("tcp", suite.server.server.Addr().String())
	require.NoError(suite.T(), err)
}

func (suite *ClientRecvRoutineSuite) TearDownTest() {
	suite.cl.Close()
	suite.server.Close()
}

func TestClientRecvRoutineSuite(t *testing.T) {
	suite.Run(t, new(ClientRecvRoutineSuite))
}

func (suite *ClientRecvRoutineSuite) TestPingPong() {
	encoded, _ := proto.Marshal(&rpc.CompleteRpcMessage{
		Header: &rpc.RpcHeader{
			Mode: rpc.RpcMode_PING.Enum(),
		},
		ProtobufBody: []byte("ping"),
	})

	suite.cl.outbound <- encoded
	<-suite.cl.pingpong
}

func (suite *ClientRecvRoutineSuite) TestCloseOutbound() {
	close(suite.cl.outbound)
	time.Sleep(100 * time.Millisecond)

	// suite.Nil(suite.cl.close)
}

func (suite *ClientRecvRoutineSuite) TestWriteFail() {
	suite.cl.conn.Close()

	suite.cl.outbound <- []byte("foobar")
	time.Sleep(100 * time.Millisecond)

	_, ok := <-suite.cl.close
	suite.False(ok)
}

func (suite *ClientRecvRoutineSuite) TestRecvWithHeartBeat() {
	suite.Eventually(func() bool {
		suite.stateMtx.Lock()
		defer suite.stateMtx.Unlock()

		return suite.pings > 5
	}, 1*time.Second, 10*time.Millisecond)
}

func (suite *ClientRecvRoutineSuite) TestRecvQueryHandle() {
	c := make(chan *rpc.CompleteRpcMessage)
	suite.cl.queryMap.Store(int32(1), c)

	encoded, _ := proto.Marshal(&rpc.CompleteRpcMessage{
		ProtobufBody: []byte("handle"),
	})

	suite.cl.outbound <- encoded
	msg := <-c

	suite.EqualValues(rpc.RpcMode_RESPONSE, msg.Header.GetMode())
	suite.EqualValues(user.RpcType_QUERY_HANDLE, msg.Header.GetRpcType())
	suite.EqualValues(1, msg.Header.GetCoordinationId())

	q := &shared.QueryId{}
	suite.NoError(proto.Unmarshal(msg.GetProtobufBody(), q))
	suite.EqualValues(123456, q.GetPart1())
	suite.EqualValues(98765, q.GetPart2())
}

func (suite *ClientRecvRoutineSuite) TestRecvHandleNoChannel() {
	encoded, _ := proto.Marshal(&rpc.CompleteRpcMessage{
		ProtobufBody: []byte("handle"),
	})

	suite.cl.outbound <- encoded
	// make sure no crash
	time.Sleep(100 * time.Millisecond)
}

func (suite *ClientRecvRoutineSuite) TestRecvQueryData() {
	c := make(chan *queryData)
	suite.cl.resultMap.Store(qid{123456, 98765}, c)

	encoded, _ := proto.Marshal(&rpc.CompleteRpcMessage{
		ProtobufBody: []byte("data"),
	})

	suite.cl.outbound <- encoded
	qd := <-c

	suite.EqualValues(user.RpcType_QUERY_DATA, qd.typ)
	suite.Equal(deadbeef, qd.raw)

	data := qd.msg.(*shared.QueryData)
	suite.EqualValues(5, data.GetRowCount())
}

func (suite *ClientRecvRoutineSuite) TestRecvQueryResult() {
	c := make(chan *queryData)
	suite.cl.resultMap.Store(qid{123456, 98765}, c)

	encoded, _ := proto.Marshal(&rpc.CompleteRpcMessage{
		ProtobufBody: []byte("result"),
	})

	suite.cl.outbound <- encoded
	qr := <-c

	suite.EqualValues(user.RpcType_QUERY_RESULT, qr.typ)
	suite.Empty(qr.raw)

	data := qr.msg.(*shared.QueryResult)
	suite.EqualValues(shared.QueryResult_COMPLETED, data.GetQueryState())
}

func (suite *ClientRecvRoutineSuite) TestRecvBadQueryData() {
	c := make(chan *queryData)
	suite.cl.resultMap.Store(qid{123456, 98765}, c)

	encoded, _ := proto.Marshal(&rpc.CompleteRpcMessage{
		ProtobufBody: []byte("baddata"),
	})

	suite.cl.outbound <- encoded
	suite.Never(func() bool {
		<-c
		return true
	}, 1*time.Second, 100*time.Millisecond)
}

func (suite *ClientRecvRoutineSuite) TestRecvBadQueryResult() {
	c := make(chan *queryData)
	suite.cl.resultMap.Store(qid{123456, 98765}, c)

	encoded, _ := proto.Marshal(&rpc.CompleteRpcMessage{
		ProtobufBody: []byte("badresult"),
	})

	suite.cl.outbound <- encoded
	suite.Never(func() bool {
		<-c
		return true
	}, 1*time.Second, 100*time.Millisecond)
}
