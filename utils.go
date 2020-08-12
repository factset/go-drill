package drill

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"google.golang.org/protobuf/proto"
)

func makePrefixedMessage(data []byte) []byte {
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
		return nil, fmt.Errorf("invalid response")
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

	return data.GetRawRPCMessage(respBytes)
}

func readPrefixedMessage(r io.Reader, msg proto.Message) (*rpc.RpcHeader, error) {
	respBytes, err := readPrefixed(r)
	if err != nil {
		return nil, err
	}

	return data.DecodeRpcMessage(respBytes, msg)
}
