package drill

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
)

type mockConn struct {
	r io.Reader
	mock.Mock
}

func (m *mockConn) Close() error {
	return m.Called().Error(0)
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(_ time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(_ time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(_ time.Time) error { return nil }

func (m *mockConn) Read(b []byte) (int, error) {
	m.Called()
	return m.r.Read(b)
}

func (m *mockConn) Write(b []byte) (int, error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

type mockEncoder struct {
	mock.Mock
}

func (m *mockEncoder) WriteRaw(_ net.Conn, b []byte) (int, error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

func (m *mockEncoder) Write(_ net.Conn, mode rpc.RpcMode, typ user.RpcType, coord int32, msg proto.Message) (int, error) {
	val, _ := proto.Marshal(msg)
	args := m.Called(mode, typ, coord, val)
	return args.Int(0), args.Error(1)
}

func (m *mockEncoder) ReadMsg(_ net.Conn, msg proto.Message) (*rpc.RpcHeader, error) {
	args := m.Called(msg)
	return args.Get(0).(*rpc.RpcHeader), args.Error(1)
}

func (m *mockEncoder) ReadRaw(net.Conn) (*rpc.CompleteRpcMessage, error) {
	args := m.Called()
	return args.Get(0).(*rpc.CompleteRpcMessage), args.Error(1)
}

var initialUserToBit = []byte{0x8, 0x2, 0x10, 0x1, 0x18, 0x5, 0x2a, 0xc, 0xa, 0xa, 0xa, 0x6, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x12, 0x0, 0x30, 0x0, 0x38, 0x1, 0x42, 0x2c, 0xa, 0x1a, 0x41, 0x70, 0x61, 0x63, 0x68, 0x65, 0x20, 0x44, 0x72, 0x69, 0x6c, 0x6c, 0x20, 0x47, 0x6f, 0x6c, 0x61, 0x6e, 0x67, 0x20, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x12, 0x6, 0x31, 0x2e, 0x31, 0x37, 0x2e, 0x30, 0x18, 0x1, 0x20, 0x11, 0x28, 0x0, 0x32, 0x0, 0x48, 0x2}

func TestClientDoHandshake(t *testing.T) {
	tests := []struct {
		name   string
		opts   Options
		status *user.HandshakeStatus
		err    bool
		errmsg string
	}{
		{"successful", Options{}, user.HandshakeStatus_SUCCESS.Enum(), false, ""},
		{"rpc mismatch", Options{}, user.HandshakeStatus_RPC_VERSION_MISMATCH.Enum(), true, "invalid rpc version, expected: 5, actual: 10"},
		{"auth fail", Options{}, user.HandshakeStatus_AUTH_FAILED.Enum(), true, "authentication failure"},
		{"unknown failure", Options{}, user.HandshakeStatus_UNKNOWN_FAILURE.Enum(), true, "unknown handshake failure"},
		{"invalid security", Options{SaslEncrypt: true}, user.HandshakeStatus_SUCCESS.Enum(), true, "invalid security options"},
		{"client auth, not server", Options{Auth: "booya"}, user.HandshakeStatus_SUCCESS.Enum(), true, "client wanted auth, but server didn't require it"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := new(mockEncoder)
			m.Test(t)
			defer m.AssertExpectations(t)

			m.On("Write", rpc.RpcMode_REQUEST, user.RpcType_HANDSHAKE, int32(1), initialUserToBit).Return(0, nil)
			m.On("ReadMsg", mock.AnythingOfType("*user.BitToUserHandshake")).Return((*rpc.RpcHeader)(nil), nil).Run(func(args mock.Arguments) {
				info := args.Get(0).(*user.BitToUserHandshake)
				info.Status = tt.status
				info.RpcVersion = proto.Int32(10)
			})

			cl := Client{dataEncoder: m, coordID: 1, Opts: tt.opts}
			err := cl.doHandshake()
			if !tt.err {
				assert.NoError(t, err)
				return
			}

			assert.EqualError(t, err, tt.errmsg)
		})
	}
}

func TestClientHandshakeWriteFailure(t *testing.T) {
	m := new(mockEncoder)
	m.Test(t)
	defer m.AssertExpectations(t)

	m.On("Write", rpc.RpcMode_REQUEST, user.RpcType_HANDSHAKE, int32(1), initialUserToBit).Return(0, assert.AnError)
	cl := Client{dataEncoder: m, coordID: 1}
	assert.Same(t, assert.AnError, cl.doHandshake())
}

func TestClientHandshakeReadFailure(t *testing.T) {
	m := new(mockEncoder)
	m.Test(t)
	defer m.AssertExpectations(t)

	m.On("Write", rpc.RpcMode_REQUEST, user.RpcType_HANDSHAKE, int32(1), initialUserToBit).Return(0, nil)
	m.On("ReadMsg", mock.AnythingOfType("*user.BitToUserHandshake")).Return((*rpc.RpcHeader)(nil), assert.AnError)

	cl := Client{dataEncoder: m, coordID: 1}
	assert.Same(t, assert.AnError, cl.doHandshake())
}
