package drill

import (
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/factset/go-drill/internal/rpc/proto/exec"
	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
	"github.com/factset/go-drill/sasl"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

var initialUserToBit = []byte{0x8, 0x2, 0x10, 0x1, 0x18, 0x5, 0x22, 0x2, 0xa, 0x0, 0x2a, 0x1a, 0xa, 0xa, 0xa, 0x6, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x12, 0x0, 0xa, 0xc, 0xa, 0x8, 0x75, 0x73, 0x65, 0x72, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x0, 0x30, 0x0, 0x38, 0x1, 0x42, 0x2c, 0xa, 0x1a, 0x41, 0x70, 0x61, 0x63, 0x68, 0x65, 0x20, 0x44, 0x72, 0x69, 0x6c, 0x6c, 0x20, 0x47, 0x6f, 0x6c, 0x61, 0x6e, 0x67, 0x20, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x12, 0x6, 0x31, 0x2e, 0x31, 0x37, 0x2e, 0x30, 0x18, 0x1, 0x20, 0x11, 0x28, 0x0, 0x32, 0x0, 0x48, 0x2}

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
		{"calls handle auth", Options{Auth: "booya"}, user.HandshakeStatus_AUTH_REQUIRED.Enum(), true, "client wants encryption, server doesn't support encryption"},
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

type mockWrapper struct {
	mock.Mock
}

func (m *mockWrapper) InitAuthPayload() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *mockWrapper) Step(b []byte) ([]byte, gssapi.Status) {
	args := m.Called(b)
	return args.Get(0).([]byte), args.Get(1).(gssapi.Status)
}

func (m *mockWrapper) GetWrappedConn(c net.Conn) net.Conn {
	return m.Called(c).Get(0).(net.Conn)
}

func TestClientHandleAuth(t *testing.T) {
	defer func(orig func(string, string, sasl.SecurityProps) (sasl.Wrapper, error)) {
		createSasl = orig
	}(createSasl)

	opts := Options{
		ServiceHost: "hoster",
		User:        "edelgard",
		ServiceName: "fire emblem",
		Auth:        "kerberos",
		SaslEncrypt: true,
	}

	hostopts := Options{
		ServiceHost: "_HOST",
		Auth:        "kerberos",
		User:        "kirby",
		ServiceName: "superstar",
	}

	serverInfo := &user.BitToUserHandshake{
		MaxWrappedSize: proto.Int32(6555),
		Encrypted:      proto.Bool(true),
	}

	tests := []struct {
		name     string
		opts     Options
		saslHost string
		sinfo    *user.BitToUserHandshake
		errWhere string
	}{
		{"successful test", opts, "fire emblem/hoster", serverInfo, ""},
		{"check _HOST", hostopts, "superstar/adder.com", serverInfo, ""},
		{"createSasl errors", opts, "fire emblem/hoster", serverInfo, "createSasl"},
		{"InitAuthPayload error", opts, "fire emblem/hoster", serverInfo, "initauth"},
		{"read start fail", opts, "fire emblem/hoster", serverInfo, "saslStart"},
		{"step status error", opts, "fire emblem/hoster", serverInfo, "stepStatus"},
		{"sasl read error", opts, "fire emblem/hoster", serverInfo, "saslRead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper := new(mockWrapper)
			wrapper.Test(t)

			enc := new(mockEncoder)
			enc.Test(t)
			if tt.errWhere == "" {
				defer wrapper.AssertExpectations(t)
				defer enc.AssertExpectations(t)
			}

			createSasl = func(user, service string, props sasl.SecurityProps) (sasl.Wrapper, error) {
				assert.Equal(t, tt.opts.User, user)
				assert.Equal(t, tt.saslHost, service)
				assert.Equal(t, sasl.SecurityProps{
					MinSsf:        56,
					MaxSsf:        math.MaxUint32,
					MaxBufSize:    *tt.sinfo.MaxWrappedSize,
					UseEncryption: *tt.sinfo.Encrypted,
				}, props)

				if tt.errWhere == "createSasl" {
					return nil, assert.AnError
				}

				return wrapper, nil
			}

			cl := Client{
				dataEncoder: enc,
				Opts:        tt.opts,
				serverInfo:  tt.sinfo,
				endpoint:    &exec.DrillbitEndpoint{Address: proto.String("adder.com")},
			}

			if tt.errWhere == "initauth" {
				wrapper.On("InitAuthPayload").Return([]byte{}, assert.AnError)
			} else {
				// first we'll get the initialization auth payload
				wrapper.On("InitAuthPayload").Return(deadbeef, nil).Once()
			}

			// then we write that same payload to the socket, wrapped with a SASL_START message
			enc.On("Write", rpc.RpcMode_REQUEST, user.RpcType_SASL_MESSAGE, int32(0),
				[]byte{0xa, 0x8, 0x6b, 0x65, 0x72, 0x62, 0x65, 0x72, 0x6f, 0x73, 0x12, 0x4, 0xde, 0xad, 0xbe, 0xef, 0x18, 0x1}).Return(0, nil).Once()

			// then we receive a response which we're gonna pass the Data value to Step
			call := enc.On("ReadMsg", mock.AnythingOfType("*shared.SaslMessage")).Run(func(args mock.Arguments) {
				msg := args.Get(0).(*shared.SaslMessage)
				msg.Status = shared.SaslStatus_SASL_IN_PROGRESS.Enum()
				msg.Data = deadbeef
			}).Once()
			if tt.errWhere == "saslStart" {
				call.Return((*rpc.RpcHeader)(nil), assert.AnError)
			} else {
				call.Return((*rpc.RpcHeader)(nil), nil)
			}

			if tt.errWhere == "stepStatus" {
				wrapper.On("Step", deadbeef).Return(append(deadbeef, deadbeef...), gssapi.Status{Code: gssapi.StatusFailure}).Once()
			} else {
				// Step the wrapper to get the next token to return
				wrapper.On("Step", deadbeef).Return(append(deadbeef, deadbeef...), gssapi.Status{Code: gssapi.StatusContinueNeeded}).Once()
			}
			// Write that request wrapped correctly again
			enc.On("Write", rpc.RpcMode_REQUEST, user.RpcType_SASL_MESSAGE, int32(1),
				[]byte{0x12, 0x8, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0x18, 0x2}).Return(10, nil).Once()

			// this response has a different payload to ensure we use the new payload in the next step
			enc.On("ReadMsg", mock.AnythingOfType("*shared.SaslMessage")).Return((*rpc.RpcHeader)(nil), nil).Run(func(args mock.Arguments) {
				msg := args.Get(0).(*shared.SaslMessage)
				msg.Status = shared.SaslStatus_SASL_IN_PROGRESS.Enum()
				msg.Data = append(deadbeef, deadbeef...)
			}).Once()

			// the step gets the new payload and returns that we've completed our auth
			wrapper.On("Step", append(deadbeef, deadbeef...)).Return([]byte{}, gssapi.Status{Code: gssapi.StatusComplete}).Once()
			// write the auth complete message with SASL_SUCCESS
			enc.On("Write", rpc.RpcMode_REQUEST, user.RpcType_SASL_MESSAGE, int32(2), []byte{0x12, 0x0, 0x018, 0x3}).Return(0, nil).Once()

			if tt.errWhere == "saslRead" {
				enc.On("ReadMsg", mock.AnythingOfType("*shared.SaslMessage")).Return((*rpc.RpcHeader)(nil), assert.AnError)
			} else {
				// read the confirmation from the service which has SASL_SUCCESS which breaks the loop
				enc.On("ReadMsg", mock.AnythingOfType("*shared.SaslMessage")).Return((*rpc.RpcHeader)(nil), nil).Run(func(args mock.Arguments) {
					msg := args.Get(0).(*shared.SaslMessage)
					msg.Status = shared.SaslStatus_SASL_SUCCESS.Enum()
				}).Once()
			}

			m := new(mockConn)
			// make sure we are wrapping the connection in the client
			wrapper.On("GetWrappedConn", nil).Return(m)

			err := cl.handleAuth()
			if tt.errWhere == "" {
				assert.NoError(t, err)
				assert.Same(t, m, cl.conn)
			} else {
				if tt.errWhere == "stepStatus" {
					assert.EqualError(t, err, gssapi.Status{Code: gssapi.StatusFailure}.Error())
				} else {
					assert.Same(t, assert.AnError, err)
				}
				assert.Nil(t, cl.conn)
			}
		})
	}
}
