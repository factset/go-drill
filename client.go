package drill

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"

	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"github.com/zeroshade/go-drill/sasl"
	"google.golang.org/protobuf/proto"
)

//go:generate go run ./internal/cmd/drillProto runall ../../../

type drillClient struct {
	conn net.Conn

	opts Options

	drillBits []string
	curBit    int
	coordID   int32
	endpoint  *exec.DrillbitEndpoint
	zkNodes   []string

	serverInfo *user.BitToUserHandshake
}

func makePrefixedMessage(data []byte) []byte {
	buf := make([]byte, binary.MaxVarintLen32)
	nbytes := binary.PutUvarint(buf, uint64(len(data)))
	return append(buf[:nbytes], data...)
}

func readPrefixedRaw(r io.Reader) (*rpc.CompleteRpcMessage, error) {
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

	return data.GetRawRPCMessage(respBytes)
}

func readPrefixedMessage(r io.Reader, msg proto.Message) (*rpc.RpcHeader, error) {
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

	return data.DecodeRpcMessage(respBytes, msg)
}

func (d *drillClient) nextCoordID() (next int32) {
	next = d.coordID
	d.coordID++
	return
}

func (d *drillClient) sendCancel(qid *shared.QueryId) error {
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_CANCEL_QUERY, d.nextCoordID(), qid)
	if err != nil {
		return err
	}

	d.conn.Write(makePrefixedMessage(encoded))

	return nil
}

func (d *drillClient) sendAck(coordID int32, isOk bool) {
	ack := &rpc.Ack{
		Ok: &isOk,
	}

	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_RESPONSE, user.RpcType_ACK, coordID, ack)
	if err != nil {
		panic(err)
	}

	d.conn.Write(makePrefixedMessage(encoded))
}

func (d *drillClient) Close() error {
	d.conn.Close()
	return nil
}

func (d *drillClient) connectEndpoint(ctx context.Context, e *exec.DrillbitEndpoint) error {
	d.endpoint = e
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.endpoint.GetAddress()+":"+strconv.Itoa(int(d.endpoint.GetUserPort())))
	if err != nil {
		return err
	}
	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetNoDelay(true)

	d.conn = conn
	d.coordID = int32(rand.Int()%1729 + 1)
	return d.doHandshake()
}

func (d *drillClient) Connect(ctx context.Context, zkNode ...string) error {
	d.zkNodes = zkNode

	zoo, err := newZKHandler(d.opts.ClusterName, zkNode...)
	if err != nil {
		return err
	}
	defer zoo.Close()

	d.drillBits = zoo.GetDrillBits()
	rand.Shuffle(len(d.drillBits), func(i, j int) {
		d.drillBits[i], d.drillBits[j] = d.drillBits[j], d.drillBits[i]
	})

	d.curBit = 0
	return d.connectEndpoint(ctx, zoo.GetEndpoint(d.drillBits[d.curBit]))
}

func (d *drillClient) MakeMetaRequest() *user.ServerMeta {
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_GET_SERVER_META, d.nextCoordID(), &user.GetServerMetaReq{})
	if err != nil {
		panic(err)
	}

	data := makePrefixedMessage(encoded)
	d.conn.Write(data)

	resp := &user.GetServerMetaResp{}
	hdr, err := readPrefixedMessage(d.conn, resp)
	fmt.Println(hdr)
	fmt.Println(resp.GetStatus())
	fmt.Println(resp.GetError())
	return resp.ServerMeta
}

func (d *drillClient) getQueryResults(ctx context.Context, qid *shared.QueryId, b chan *rowbatch) {
	defer close(b)

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			d.sendCancel(qid)
		case <-done:
		}
	}()

	for {
		msg, err := readPrefixedRaw(d.conn)
		if err != nil {
			b <- &rowbatch{readErr: err}
			return
		}

		switch msg.GetHeader().GetRpcType() {
		case int32(user.RpcType_ACK):
			// this means a cancel was sent, we should receive a QueryResult message
			// shortly with the cancelled message
		case int32(user.RpcType_QUERY_DATA):
			d.sendAck(msg.Header.GetCoordinationId(), true)

			qd := &shared.QueryData{}
			if err = proto.Unmarshal(msg.ProtobufBody, qd); err != nil {
				panic(err)
			}

			if qid.GetPart1() != qd.QueryId.GetPart1() {
				panic("wtf query id")
			}

			// fmt.Println("Fields:", qd.GetDef())

			rb := &rowbatch{
				def:  qd.GetDef(),
				vecs: make([]data.DataVector, 0, len(qd.GetDef().GetField())),
			}

			rawData := msg.GetRawBody()
			var offset int32 = 0
			for _, f := range qd.GetDef().GetField() {
				// fmt.Println("Field #", idx, ":", f.GetNamePart().GetName())

				rb.vecs = append(rb.vecs, data.NewValueVec(rawData[offset:offset+f.GetBufferLength()], f))
				offset += f.GetBufferLength()
			}

			b <- rb
		case int32(user.RpcType_QUERY_RESULT):
			d.sendAck(*msg.Header.CoordinationId, true)

			qr := &shared.QueryResult{}
			if err = proto.Unmarshal(msg.ProtobufBody, qr); err != nil {
				panic(err)
			}

			if qid.GetPart1() != qr.GetQueryId().GetPart1() {
				panic("mismatch")
			}

			// fmt.Println(qr.GetQueryState())
			// fmt.Println(msg.GetHeader())
			b <- &rowbatch{queryResult: qr}
			return
		}
	}
}

func (d *drillClient) SubmitQuery(t shared.QueryType, plan string) (*shared.QueryId, error) {
	query := &user.RunQuery{
		ResultsMode: user.QueryResultsMode_STREAM_FULL.Enum(),
		Type:        &t,
		Plan:        &plan,
	}

	coord := d.nextCoordID()
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_RUN_QUERY, coord, query)
	if err != nil {
		return nil, err
	}

	// fmt.Println("Submit with Coord:", coord)
	_, err = d.conn.Write(makePrefixedMessage(encoded))
	if err != nil {
		return nil, err
	}

	resp := &shared.QueryId{}
	_, err = readPrefixedMessage(d.conn, resp)
	// fmt.Println(hdr)
	return resp, err
}

func (d *drillClient) doHandshake() error {
	u2b := user.UserToBitHandshake{
		Channel:             shared.RpcChannel_USER.Enum(),
		RpcVersion:          proto.Int32(drillRpcVersion),
		SupportListening:    proto.Bool(true),
		SupportTimeout:      proto.Bool(true),
		SaslSupport:         user.SaslSupport_SASL_PRIVACY.Enum(),
		SupportComplexTypes: proto.Bool(d.opts.SupportComplexTypes),
		ClientInfos: &user.RpcEndpointInfos{
			Name:         proto.String(clientName),
			Version:      proto.String(drillVersion),
			Application:  &d.opts.ApplicationName,
			MajorVersion: proto.Uint32(drillMajorVersion),
			MinorVersion: proto.Uint32(drillMinorVersion),
			PatchVersion: proto.Uint32(drillPatchVersion),
		},
		Properties: &user.UserProperties{
			Properties: []*user.Property{
				{Key: proto.String("schema"), Value: &d.opts.Schema},
			},
		},
	}

	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_HANDSHAKE, d.nextCoordID(), &u2b)
	if err != nil {
		return err
	}

	log.Print("Handshake, write: ")
	log.Println(d.conn.Write(makePrefixedMessage(encoded)))

	d.serverInfo = &user.BitToUserHandshake{}
	hdr, err := readPrefixedMessage(d.conn, d.serverInfo)
	if err != nil {
		return err
	}

	log.Println("Encrypted: ", d.serverInfo.GetEncrypted())
	log.Println("MaxWrapped: ", d.serverInfo.GetMaxWrappedSize())
	log.Println("AuthMechs: ", d.serverInfo.GetAuthenticationMechanisms())

	log.Println(d.serverInfo.GetStatus().String())
	log.Println(hdr.GetMode().String())
	log.Println(user.RpcType_name[hdr.GetRpcType()])

	switch d.serverInfo.GetStatus() {
	case user.HandshakeStatus_SUCCESS:
		if (len(d.opts.Auth) > 0 && d.opts.Auth != "plain") || d.opts.SaslEncrypt {
			return fmt.Errorf("client wanted auth, but server didn't require it")
		}
	case user.HandshakeStatus_RPC_VERSION_MISMATCH:
		return fmt.Errorf("invalid rpc version, expected: %d, actual %d", drillRpcVersion, d.serverInfo.GetRpcVersion())
	case user.HandshakeStatus_AUTH_FAILED:
		return fmt.Errorf("authentication failure")
	case user.HandshakeStatus_UNKNOWN_FAILURE:
		return fmt.Errorf("unknown handshake failure")
	case user.HandshakeStatus_AUTH_REQUIRED:
		log.Println("server requires sasl authentication")
		return d.handleAuth()
	}

	return nil
}

func (d *drillClient) handleAuth() error {
	if ((len(d.opts.Auth) > 0 && d.opts.Auth != "plain") || d.opts.SaslEncrypt) && !d.serverInfo.GetEncrypted() {
		return fmt.Errorf("client wants encryption, server doesn't support encryption")
	}

	wrapper, err := sasl.NewSaslWrapper(d.opts.User, d.opts.ServiceName+"/"+d.endpoint.GetAddress(), sasl.SecurityProps{
		MinSsf:        56,
		MaxSsf:        math.MaxUint32,
		MaxBufSize:    d.serverInfo.GetMaxWrappedSize(),
		UseEncryption: d.serverInfo.GetEncrypted(),
	})

	if err != nil {
		return err
	}

	token, err := wrapper.InitAuthPayload()
	if err != nil {
		return err
	}

	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_SASL_MESSAGE, d.nextCoordID(), &shared.SaslMessage{
		Mechanism: &d.opts.Auth,
		Data:      token,
		Status:    shared.SaslStatus_SASL_START.Enum(),
	})
	if err != nil {
		return err
	}

	d.conn.Write(makePrefixedMessage(encoded))

	saslResp := &shared.SaslMessage{}
	_, err = readPrefixedMessage(d.conn, saslResp)
	if err != nil {
		return err
	}

	for saslResp.GetStatus() == shared.SaslStatus_SASL_IN_PROGRESS {
		token, st := wrapper.Step(saslResp.GetData())
		if st.Code != gssapi.StatusContinueNeeded && st.Code != gssapi.StatusComplete {
			return errors.New(st.Error())
		}

		encodeStatus := shared.SaslStatus_SASL_IN_PROGRESS.Enum()
		if st.Code == gssapi.StatusComplete {
			encodeStatus = shared.SaslStatus_SASL_SUCCESS.Enum()
		}

		encoded, err = data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_SASL_MESSAGE, d.nextCoordID(), &shared.SaslMessage{
			Data:   token,
			Status: encodeStatus,
		})

		d.conn.Write(makePrefixedMessage(encoded))
		_, err = readPrefixedMessage(d.conn, saslResp)
		if err != nil {
			return err
		}
	}

	d.conn = wrapper.GetWrappedConn(d.conn)

	return nil
}
