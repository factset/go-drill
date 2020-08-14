package drill

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
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
