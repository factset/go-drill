package drill

import (
	"testing"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/factset/go-drill/internal/rpc/proto/exec"
	"google.golang.org/protobuf/proto"
)

type mockzk struct {
	mock.Mock
}

func (m *mockzk) Get(path string) ([]byte, *zk.Stat, error) {
	args := m.Called(path)
	return args.Get(0).([]byte), args.Get(1).(*zk.Stat), args.Error(2)
}

func (m *mockzk) Children(path string) ([]string, *zk.Stat, error) {
	args := m.Called(path)
	return args.Get(0).([]string), args.Get(1).(*zk.Stat), args.Error(2)
}

func (m *mockzk) Close() {
	m.Called()
}

func TestZKHandlerClose(t *testing.T) {
	m := new(mockzk)
	m.Test(t)
	m.On("Close")

	hdlr := zkHandler{conn: m}
	hdlr.Close()
	m.AssertExpectations(t)
}

func TestZKHandlerGetDrillBits(t *testing.T) {
	m := new(mockzk)
	m.Test(t)

	m.On("Children", "/drill/cluster").Return([]string{"a", "b", "c"}, (*zk.Stat)(nil), nil)

	hdlr := zkHandler{conn: m, Path: "/drill/cluster"}
	val := hdlr.GetDrillBits()
	assert.Equal(t, []string{"a", "b", "c"}, val)
	m.AssertExpectations(t)
}

func TestZKHandlerGetDrillBitsError(t *testing.T) {
	m := new(mockzk)
	m.Test(t)

	m.On("Children", "/drill/cluster").Return([]string{"c"}, (*zk.Stat)(nil), assert.AnError)

	hdlr := zkHandler{conn: m, Path: "/drill/cluster"}
	val := hdlr.GetDrillBits()
	assert.Equal(t, []string{"c"}, val)
	assert.Same(t, assert.AnError, hdlr.Err)

	m.AssertExpectations(t)
}

func TestZKHandlerGetEndpoint(t *testing.T) {
	m := new(mockzk)
	m.Test(t)

	service := &exec.DrillServiceInstance{
		Id: proto.String("bit"),
		Endpoint: &exec.DrillbitEndpoint{
			Address:  proto.String("foobar"),
			UserPort: proto.Int32(2020),
		},
	}

	data, _ := proto.Marshal(service)
	m.On("Get", "/drill/cluster/bit").Return(data, (*zk.Stat)(nil), nil)

	hdlr := zkHandler{conn: m, Path: "/drill/cluster"}
	bit := hdlr.GetEndpoint("bit")

	assert.Equal(t, "foobar", bit.GetAddress())
	assert.Equal(t, int32(2020), bit.GetUserPort())
	m.AssertExpectations(t)
}

func TestZKHandlerGetEndpointErr(t *testing.T) {
	m := new(mockzk)
	m.Test(t)

	m.On("Get", "/drill/cluster/bit").Return([]byte{}, (*zk.Stat)(nil), assert.AnError)
	hdlr := zkHandler{conn: m, Path: "/drill/cluster"}
	bit := hdlr.GetEndpoint("bit")
	assert.Nil(t, bit)
	assert.Same(t, assert.AnError, hdlr.Err)
}

func TestZKHandlerGetEndpointProtoErr(t *testing.T) {
	m := new(mockzk)
	m.Test(t)

	m.On("Get", "/drill/cluster/bit").Return([]byte{0x00}, (*zk.Stat)(nil), nil)
	hdlr := zkHandler{conn: m, Path: "/drill/cluster"}
	bit := hdlr.GetEndpoint("bit")
	assert.Nil(t, bit)
	assert.Error(t, hdlr.Err)
}

func TestNewZKHandlerFailConnect(t *testing.T) {
	hdlr, err := newZKHandler("")
	assert.Nil(t, hdlr)
	assert.Error(t, err)
}
