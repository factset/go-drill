package drill

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
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
	assert.ElementsMatch(t, []string{"zknode1", "zknode2", "zknode3"}, cl.zkNodes)
	assert.IsType(t, rpcEncoder{}, cl.dataEncoder)
	assert.Equal(t, opts, cl.Opts)
	assert.GreaterOrEqual(t, cl.coordID, int32(0))
}

type TCPServer struct {
	port   int32
	server net.Listener

	handShake *user.HandshakeStatus
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

			go t.handleConnection(conn)
		}
	}()
	return
}

func (t *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	enc := rpcEncoder{}

	if t.handShake == nil {
		t.handShake = user.HandshakeStatus_SUCCESS.Enum()
	}

	for {
		msg, err := enc.ReadRaw(conn)
		if err != nil {
			return
		}

		switch msg.Header.GetRpcType() {
		case int32(user.RpcType_HANDSHAKE):
			enc.Write(conn, rpc.RpcMode_RESPONSE, user.RpcType_HANDSHAKE, 1, &user.BitToUserHandshake{
				Encrypted: proto.Bool(false),
				Status:    t.handShake,
			})
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
	server := TCPServer{}

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
	server := TCPServer{}
	server.handShake = user.HandshakeStatus_UNKNOWN_FAILURE.Enum()

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

	cl := NewClient(Options{ClusterName: "drill"}, "a", "b", "c")
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

	server := TCPServer{}
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
		assert.Equal(t, "drill", clst)
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

	assert.Error(t, err)
	assert.Equal(t, cl.Opts, nc.(*Client).Opts)
	assert.EqualValues(t, cl.drillBits, nc.(*Client).drillBits)
	assert.EqualValues(t, cl.nextBit, nc.(*Client).nextBit)
}
