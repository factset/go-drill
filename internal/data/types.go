package data

import (
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"google.golang.org/protobuf/proto"
)

func EncodeRpcMessage(mode rpc.RpcMode, msgType user.RpcType, coordID int32, msg proto.Message) ([]byte, error) {
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

func DecodeRpcMessage(data []byte, msg proto.Message) (*rpc.RpcHeader, error) {
	rpcMsg := &rpc.CompleteRpcMessage{}
	if err := proto.Unmarshal(data, rpcMsg); err != nil {
		return nil, err
	}

	ret := rpcMsg.GetHeader()
	return ret, proto.Unmarshal(rpcMsg.ProtobufBody, msg)
}

func GetRawRPCMessage(data []byte) (*rpc.CompleteRpcMessage, error) {
	rpcMsg := &rpc.CompleteRpcMessage{}
	if err := proto.Unmarshal(data, rpcMsg); err != nil {
		return nil, err
	}

	return rpcMsg, nil
}
