package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/factset/go-drill"
)

type mockDrillClient struct {
	mock.Mock
}

func (m *mockDrillClient) NewConnection(ctx context.Context) (drill.Conn, error) {
	args := m.Called(ctx)
	return args.Get(0).(drill.Conn), args.Error(1)
}

func (m *mockDrillClient) GetEndpoint() drill.Drillbit                           { return nil }
func (m *mockDrillClient) Connect(context.Context) error                         { return nil }
func (m *mockDrillClient) ConnectEndpoint(context.Context, drill.Drillbit) error { return nil }
func (m *mockDrillClient) ConnectWithZK(context.Context, ...string) error        { return nil }
func (m *mockDrillClient) Ping(context.Context) error                            { return nil }
func (m *mockDrillClient) Close() error                                          { return nil }
func (m *mockDrillClient) ExecuteStmt(p drill.PreparedHandle) (drill.DataHandler, error) {
	args := m.Called(p)
	return args.Get(0).(drill.DataHandler), args.Error(1)
}
func (m *mockDrillClient) SubmitQuery(t drill.QueryType, query string) (drill.DataHandler, error) {
	args := m.Called(t, query)
	return args.Get(0).(drill.DataHandler), args.Error(1)
}
func (m *mockDrillClient) PrepareQuery(query string) (drill.PreparedHandle, error) {
	args := m.Called(query)
	return args.Get(0).(drill.PreparedHandle), args.Error(1)
}

func TestParseConnectStrZKDirect(t *testing.T) {
	tests := []struct {
		name     string
		testStr  string
		expected []string
		host     string
		port     int32
		err      error
	}{
		{"simple zk", "zk=node1,node2,node3", []string{"node1", "node2", "node3"}, "", 0, nil},
		{"simple direct", "host=localhost;port=8080", nil, "localhost", 8080, nil},
		{"invalid port", "host=localhost;port=foobar", nil, "", 0, errors.New("invalid port")},
		{"default port", "host=localhost", nil, "localhost", 31010, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := parseConnectStr(tt.testStr)
			if tt.err != nil {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, c.(*connector).base.(*drill.Client).ZkNodes)

			endpoint := c.(*connector).base.(*drill.Client).GetEndpoint()
			if len(tt.expected) > 0 {
				assert.Nil(t, endpoint)
				return
			}

			assert.Equal(t, tt.host, endpoint.GetAddress())
			assert.Equal(t, tt.port, endpoint.GetUserPort())
		})
	}
}

func TestParseConnectStr(t *testing.T) {
	durtest := new(time.Duration)
	*durtest = 5 * time.Second

	tests := []struct {
		name     string
		testStr  string
		expected drill.Options
	}{
		{"auth", "auth=kerberos", drill.Options{Auth: "kerberos"}},
		{"schema", "schema=foobar", drill.Options{Schema: "foobar"}},
		{"service", "service=nidrill", drill.Options{ServiceName: "nidrill"}},
		{"encrypt true", "encrypt=true", drill.Options{SaslEncrypt: true}},
		{"encrypt false", "encrypt=false", drill.Options{SaslEncrypt: false}},
		{"user", "user=driller", drill.Options{User: "driller"}},
		{"cluster", "cluster=supercluster", drill.Options{ClusterName: "supercluster"}},
		{"heartbeat", "heartbeat=5", drill.Options{HeartbeatFreq: durtest}},
		{"multiple opts", "auth=kerberos;user=foobar;encrypt=true", drill.Options{Auth: "kerberos", User: "foobar", SaslEncrypt: true}},
		{"zkpath", "zk=node1,node2,node3/drillbits", drill.Options{ZKPath: "/drillbits"}},
		{"user passwd", "user=driller;pass=12345", drill.Options{User: "driller", Passwd: "12345"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := parseConnectStr(tt.testStr)
			assert.NoError(t, err)

			assert.EqualValues(t, tt.expected, conn.(*connector).base.(*drill.Client).Opts)
		})
	}
}

func TestParseConnectStrInvalid(t *testing.T) {
	tests := []struct {
		name    string
		testStr string
		errMsg  string
	}{
		{"invalid format", "foo", "invalid format for connector string"},
		{"trailing semicolon doesn't work", "auth=bar;", "invalid format for connector string"},
		{"invalid encrypt val", "encrypt=foo", "strconv.ParseBool: parsing \"foo\": invalid syntax"},
		{"invalid heartbeat freq", "heartbeat=foo", "strconv.Atoi: parsing \"foo\": invalid syntax"},
		{"invalid arg", "foo=bar", "invalid argument for connection string: foo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConnectStr(tt.testStr)
			assert.Error(t, err)
			assert.EqualError(t, err, tt.errMsg)
		})
	}
}

func TestConnectorDriver(t *testing.T) {
	c := &connector{}
	assert.IsType(t, drillDriver{}, c.Driver())
}

func TestConnectorConnect(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)

	ctx := context.Background()
	m.On("NewConnection", ctx).Return(m, nil)

	c := &connector{base: m}
	cn, err := c.Connect(ctx)
	assert.NoError(t, err)
	assert.Same(t, cn.(*conn).Conn, m)
}

func TestConnectorConnectErr(t *testing.T) {
	m := new(mockDrillClient)
	m.Test(t)

	ctx := context.Background()
	m.On("NewConnection", ctx).Return(m, assert.AnError)

	c := &connector{base: m}
	conn, err := c.Connect(ctx)
	assert.Nil(t, conn)
	assert.Same(t, assert.AnError, err)
}
