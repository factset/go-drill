package drill

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

var deadbeef = []byte{0xDE, 0xAD, 0xBE, 0xEF}

func TestRpcEncoderWriteRaw(t *testing.T) {
	m := new(mockConn)
	m.Test(t)

	m.On("Write", makePrefixedMessage(deadbeef)).Return(len(deadbeef), nil)

	val, err := rpcEncoder{}.WriteRaw(m, deadbeef)
	assert.Equal(t, len(deadbeef), val)
	assert.NoError(t, err)
	m.AssertExpectations(t)
}

func TestRpcEncoderWriteRawErr(t *testing.T) {
	m := new(mockConn)
	m.Test(t)

	m.On("Write", makePrefixedMessage(deadbeef)).Return(0, assert.AnError)
	val, err := rpcEncoder{}.WriteRaw(m, deadbeef)
	assert.Equal(t, 0, val)
	assert.Same(t, assert.AnError, err)
	m.AssertExpectations(t)
}

func TestRpcEncoderWriteMsg(t *testing.T) {
	m := new(mockConn)
	m.Test(t)

	enc, _ := encodeRPCMessage(rpc.RpcMode_PING, user.RpcType_ACK, 1, &shared.QueryId{})

	m.On("Write", makePrefixedMessage(enc)).Return(len(enc), nil)
	val, err := rpcEncoder{}.Write(m, rpc.RpcMode_PING, user.RpcType_ACK, 1, &shared.QueryId{})
	assert.Equal(t, len(enc), val)
	assert.NoError(t, err)
	m.AssertExpectations(t)
}

func TestRpcEncoderWriteMsgErr(t *testing.T) {
	m := new(mockConn)
	m.Test(t)

	enc, _ := encodeRPCMessage(rpc.RpcMode_PING, user.RpcType_ACK, 1, &shared.QueryId{})

	m.On("Write", makePrefixedMessage(enc)).Return(0, assert.AnError)
	val, err := rpcEncoder{}.Write(m, rpc.RpcMode_PING, user.RpcType_ACK, 1, &shared.QueryId{})
	assert.Equal(t, 0, val)
	assert.Same(t, assert.AnError, err)
	m.AssertExpectations(t)
}

func TestRpcEncoderReadRaw(t *testing.T) {
	m := new(mockConn)
	m.Test(t)

	msg := &rpc.CompleteRpcMessage{
		Header: &rpc.RpcHeader{
			Mode:           rpc.RpcMode_PING.Enum(),
			CoordinationId: proto.Int(5),
			RpcType:        proto.Int(5),
		},
		ProtobufBody: deadbeef,
	}

	data, _ := proto.Marshal(msg)
	buf := make([]byte, binary.MaxVarintLen32)
	nb := binary.PutUvarint(buf, uint64(len(data)))
	m.r = bytes.NewReader(append(buf[:nb], data...))

	m.On("Read")
	out, err := rpcEncoder{}.ReadRaw(m)
	assert.NoError(t, err)

	assert.Equal(t, msg.Header.GetMode(), out.Header.GetMode())
	assert.Equal(t, msg.Header.GetCoordinationId(), out.Header.GetCoordinationId())
	assert.Equal(t, msg.Header.GetRpcType(), out.Header.GetRpcType())
	assert.Equal(t, msg.GetProtobufBody(), out.GetProtobufBody())
	m.AssertExpectations(t)
}

func TestRpcEncoderReadMsg(t *testing.T) {
	qid := &shared.QueryId{Part1: proto.Int64(12345), Part2: proto.Int64(98765)}
	encoded, _ := proto.Marshal(qid)
	msg := &rpc.CompleteRpcMessage{
		Header: &rpc.RpcHeader{
			Mode:           rpc.RpcMode_PING.Enum(),
			CoordinationId: proto.Int(5),
			RpcType:        proto.Int(5),
		},
		ProtobufBody: encoded,
	}

	data, _ := proto.Marshal(msg)
	buf := make([]byte, binary.MaxVarintLen32)
	nb := binary.PutUvarint(buf, uint64(len(data)))

	m := new(mockConn)
	m.Test(t)
	m.r = bytes.NewReader(append(buf[:nb], data...))

	m.On("Read")
	out := &shared.QueryId{}
	hdr, err := rpcEncoder{}.ReadMsg(m, out)
	assert.NoError(t, err)

	assert.Equal(t, msg.Header.GetMode(), hdr.GetMode())
	assert.Equal(t, msg.Header.GetCoordinationId(), hdr.GetCoordinationId())
	assert.Equal(t, msg.Header.GetRpcType(), hdr.GetRpcType())
	assert.Equal(t, qid.GetPart1(), out.GetPart1())
	assert.Equal(t, qid.GetPart2(), out.GetPart2())
}

func TestMakePrefixedMessage(t *testing.T) {
	out := makePrefixedMessage(deadbeef)
	val, nb := binary.Uvarint(out)

	assert.EqualValues(t, len(deadbeef), val)
	assert.Len(t, out, len(deadbeef)+nb)
}

func TestMakePrefixedNil(t *testing.T) {
	assert.Nil(t, makePrefixedMessage(nil))
}

func TestReadPrefixedSimple(t *testing.T) {
	buf := make([]byte, binary.MaxVarintLen32)
	nb := binary.PutUvarint(buf, 4)

	out, err := readPrefixed(bytes.NewReader(append(buf[:nb], deadbeef...)))
	assert.NoError(t, err)
	assert.EqualValues(t, deadbeef, out)
}

func TestReadPrefixedEof(t *testing.T) {
	buf := &bytes.Reader{}
	out, err := readPrefixed(buf)
	assert.Nil(t, out)
	assert.Same(t, io.ErrUnexpectedEOF, err)
}

func TestReadPrefixedShortRead(t *testing.T) {
	buf := []byte{0x01}
	out, err := readPrefixed(bytes.NewReader(buf))
	assert.Nil(t, out)
	assert.Same(t, io.ErrUnexpectedEOF, err)
}

func TestReadPrefixedEmpty(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0}
	out, err := readPrefixed(bytes.NewBuffer(buf))
	assert.Nil(t, out)
	assert.Same(t, errInvalidResponse, err)
}

func TestReadPrefixedNotEnough(t *testing.T) {
	buf := make([]byte, binary.MaxVarintLen32)
	nb := binary.PutUvarint(buf, 6)

	out, err := readPrefixed(bytes.NewReader(append(buf[:nb], deadbeef...)))
	assert.Nil(t, out)
	assert.Same(t, io.ErrUnexpectedEOF, err)
}

func TestReadPrefixedRaw(t *testing.T) {
	msg := &rpc.CompleteRpcMessage{
		Header: &rpc.RpcHeader{
			Mode:           rpc.RpcMode_PING.Enum(),
			CoordinationId: proto.Int(5),
			RpcType:        proto.Int(5),
		},
		ProtobufBody: deadbeef,
	}

	data, _ := proto.Marshal(msg)
	buf := make([]byte, binary.MaxVarintLen32)
	nb := binary.PutUvarint(buf, uint64(len(data)))

	out, err := readPrefixedRaw(bytes.NewReader(append(buf[:nb], data...)))
	assert.NoError(t, err)

	assert.Equal(t, msg.Header.GetMode(), out.Header.GetMode())
	assert.Equal(t, msg.Header.GetCoordinationId(), out.Header.GetCoordinationId())
	assert.Equal(t, msg.Header.GetRpcType(), out.Header.GetRpcType())
	assert.Equal(t, msg.GetProtobufBody(), out.GetProtobufBody())
}

func TestReadPrefixedRawErr(t *testing.T) {
	buf := &bytes.Reader{}
	out, err := readPrefixedRaw(buf)
	assert.Nil(t, out)
	assert.Error(t, err)
}

func TestReadPrefixedMessage(t *testing.T) {
	qid := &shared.QueryId{Part1: proto.Int64(12345), Part2: proto.Int64(98765)}
	encoded, _ := proto.Marshal(qid)
	msg := &rpc.CompleteRpcMessage{
		Header: &rpc.RpcHeader{
			Mode:           rpc.RpcMode_PING.Enum(),
			CoordinationId: proto.Int(5),
			RpcType:        proto.Int(5),
		},
		ProtobufBody: encoded,
	}

	data, _ := proto.Marshal(msg)
	buf := make([]byte, binary.MaxVarintLen32)
	nb := binary.PutUvarint(buf, uint64(len(data)))

	out := &shared.QueryId{}
	hdr, err := readPrefixedMessage(bytes.NewReader(append(buf[:nb], data...)), out)
	assert.NoError(t, err)

	assert.Equal(t, msg.Header.GetMode(), hdr.GetMode())
	assert.Equal(t, msg.Header.GetCoordinationId(), hdr.GetCoordinationId())
	assert.Equal(t, msg.Header.GetRpcType(), hdr.GetRpcType())
	assert.Equal(t, qid.GetPart1(), out.GetPart1())
	assert.Equal(t, qid.GetPart2(), out.GetPart2())
}

func TestReadPrefixedMessageErr(t *testing.T) {
	buf := &bytes.Reader{}
	out, err := readPrefixedMessage(buf, nil)
	assert.Nil(t, out)
	assert.Error(t, err)
}

func TestArrowFromColPanic(t *testing.T) {
	c := &user.ColumnMetadata{}
	assert.Panics(t, func() { ColMetaToArrowField(c) })
}

func TestArrowFromCol(t *testing.T) {
	tests := []struct {
		name     string
		typ      string
		expected arrow.DataType
		nullable bool
		def      string
	}{
		{"bool", "BOOLEAN", arrow.FixedWidthTypes.Boolean, true, "false"},
		{"binary", "BINARY VARYING", arrow.BinaryTypes.Binary, false, ""},
		{"varchar", "CHARACTER VARYING", arrow.BinaryTypes.String, true, "foo"},
		{"integer", "INTEGER", arrow.PrimitiveTypes.Int32, false, "1"},
		{"int64", "BIGINT", arrow.PrimitiveTypes.Int64, true, "123456"},
		{"int16", "SMALLINT", arrow.PrimitiveTypes.Int16, false, "65535"},
		{"tinyint", "TINYINT", arrow.PrimitiveTypes.Int8, true, "1"},
		{"date", "DATE", arrow.FixedWidthTypes.Date64, false, "1987-08-04"},
		{"time", "TIME", arrow.FixedWidthTypes.Time32ms, true, "12:30PM"},
		{"float", "FLOAT", arrow.PrimitiveTypes.Float32, false, "1.2"},
		{"double", "DOUBLE", arrow.PrimitiveTypes.Float64, false, "1.2"},
		{"timestamp", "TIMESTAMP", arrow.FixedWidthTypes.Timestamp_ms, true, "123456789"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &user.ColumnMetadata{
				ColumnName:   &tt.name,
				DataType:     &tt.typ,
				IsNullable:   &tt.nullable,
				DefaultValue: &tt.def,
			}

			f := ColMetaToArrowField(c)
			assert.True(t, f.Equal(arrow.Field{
				Name:     tt.name,
				Type:     tt.expected,
				Nullable: tt.nullable,
				Metadata: arrow.NewMetadata([]string{"default"}, []string{tt.def}),
			}))
		})
	}
}
