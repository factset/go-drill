package drill

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/apache/arrow/go/arrow"
	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
)

type encoder interface {
	WriteRaw(net.Conn, []byte) (int, error)
	Write(net.Conn, rpc.RpcMode, user.RpcType, int32, proto.Message) (int, error)
	ReadMsg(net.Conn, proto.Message) (*rpc.RpcHeader, error)
	ReadRaw(net.Conn) (*rpc.CompleteRpcMessage, error)
}

type rpcEncoder struct{}

func (rpcEncoder) WriteRaw(conn net.Conn, b []byte) (int, error) {
	return conn.Write(makePrefixedMessage(b))
}

func (rpcEncoder) Write(conn net.Conn, mode rpc.RpcMode, typ user.RpcType, coord int32, msg proto.Message) (int, error) {
	encoded, err := encodeRPCMessage(mode, typ, coord, msg)
	if err != nil {
		return 0, err
	}
	return conn.Write(makePrefixedMessage(encoded))
}

func (rpcEncoder) ReadRaw(conn net.Conn) (*rpc.CompleteRpcMessage, error) {
	return readPrefixedRaw(conn)
}

func (rpcEncoder) ReadMsg(conn net.Conn, msg proto.Message) (*rpc.RpcHeader, error) {
	return readPrefixedMessage(conn, msg)
}

var errInvalidResponse = errors.New("invalid response")

func makePrefixedMessage(data []byte) []byte {
	if data == nil {
		return nil
	}

	buf := make([]byte, binary.MaxVarintLen32)
	nbytes := binary.PutUvarint(buf, uint64(len(data)))
	return append(buf[:nbytes], data...)
}

func readPrefixed(r io.Reader) ([]byte, error) {
	vbytes := make([]byte, binary.MaxVarintLen32)
	n, err := io.ReadAtLeast(r, vbytes, binary.MaxVarintLen32)
	if err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, err
	}

	respLength, vlength := binary.Uvarint(vbytes)

	// if we got an empty message and read too many bytes we're screwed
	// but this shouldn't happen anyways, just in case
	if vlength < 1 || vlength+int(respLength) < n {
		return nil, errInvalidResponse
	}

	respBytes := make([]byte, respLength)
	extraLen := copy(respBytes, vbytes[vlength:])
	_, err = io.ReadFull(r, respBytes[extraLen:])
	if err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, err
	}

	return respBytes, nil
}

func readPrefixedRaw(r io.Reader) (*rpc.CompleteRpcMessage, error) {
	respBytes, err := readPrefixed(r)
	if err != nil {
		return nil, err
	}

	return getRawRPCMessage(respBytes)
}

func readPrefixedMessage(r io.Reader, msg proto.Message) (*rpc.RpcHeader, error) {
	respBytes, err := readPrefixed(r)
	if err != nil {
		return nil, err
	}

	return decodeRPCMessage(respBytes, msg)
}

func encodeRPCMessage(mode rpc.RpcMode, msgType user.RpcType, coordID int32, msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	rpcMsg := &rpc.CompleteRpcMessage{
		Header: &rpc.RpcHeader{
			Mode:           &mode,
			CoordinationId: &coordID,
			RpcType:        proto.Int32(int32(msgType)),
		},
		ProtobufBody: data,
	}

	return proto.Marshal(rpcMsg)
}

func getRawRPCMessage(data []byte) (*rpc.CompleteRpcMessage, error) {
	rpcMsg := &rpc.CompleteRpcMessage{}
	if err := proto.Unmarshal(data, rpcMsg); err != nil {
		return nil, err
	}

	return rpcMsg, nil
}

func decodeRPCMessage(data []byte, msg proto.Message) (*rpc.RpcHeader, error) {
	rpcMsg, err := getRawRPCMessage(data)
	if err != nil {
		return nil, err
	}

	ret := rpcMsg.GetHeader()
	return ret, proto.Unmarshal(rpcMsg.ProtobufBody, msg)
}

type ColumnMeta interface {
	GetColumnName() string
	GetIsNullable() bool
	GetDataType() string
	GetCharMaxLength() int32
	GetCharOctetLength() int32
	GetNumericPrecision() int32
	GetNumericPrecisionRadix() int32
	GetNumericScale() int32
	GetDateTimePrecision() int32
	GetIntervalType() string
	GetIntervalPrecision() int32
	GetColumnSize() int32
	GetDefaultValue() string
}

func arrowDataTypeFromCol(c ColumnMeta) arrow.DataType {
	switch c.GetDataType() {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "BINARY VARYING":
		return arrow.BinaryTypes.Binary
	case "CHARACTER VARYING":
		return arrow.BinaryTypes.String
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "DATE":
		return arrow.FixedWidthTypes.Date64
	case "TIME":
		return arrow.FixedWidthTypes.Time32ms
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64
	case "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_ms
	default:
		panic("arrow type conversion not found for: " + c.GetDataType())
	}
}

// ColMetaToArrowField returns an arrow.Field for the column metadata provided,
// panics if not of type BOOLEAN, VARCHAR, VARBINARY, INTEGER, SMALLINT, BIGINT,
// TINYINT, DATE, TIME, TIMESTAMP, FLOAT, or DOUBLE.
//
// TODO: handle decimal types
//
// Adds the following metadata:
//	Default Value: key "default"
//
func ColMetaToArrowField(c ColumnMeta) arrow.Field {
	return arrow.Field{
		Name:     c.GetColumnName(),
		Nullable: c.GetIsNullable(),
		Metadata: arrow.NewMetadata([]string{"default"}, []string{c.GetDefaultValue()}),
		Type:     arrowDataTypeFromCol(c),
	}
}
