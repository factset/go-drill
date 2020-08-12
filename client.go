package drill

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/zeroshade/go-drill/internal/data"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/shared"
	"github.com/zeroshade/go-drill/internal/rpc/proto/exec/user"
	"github.com/zeroshade/go-drill/sasl"
	"google.golang.org/protobuf/proto"
)

//go:generate go run ./internal/cmd/drillProto runall ../../../

type qid struct {
	part1 int64
	part2 int64
}

type queryData struct {
	typ int32
	msg proto.Message
	raw []byte
}

type Drillbit interface {
	GetAddress() string
	GetUserPort() int32
}

type Client interface {
	Close() error
	ConnectEndpoint(context.Context, Drillbit) error
	Connect(context.Context) error
	ConnectWithZK(context.Context, ...string) error
	SubmitQuery(shared.QueryType, string) (*ResultHandle, error)
	PrepareQuery(string) (driver.Stmt, error)
	Ping(context.Context) error
}

type drillClient struct {
	conn net.Conn
	opts Options

	drillBits []string
	curBit    int
	coordID   int32
	endpoint  Drillbit
	zkNodes   []string

	serverInfo      *user.BitToUserHandshake
	cancelHeartBeat context.CancelFunc

	resultMap sync.Map
	queryMap  sync.Map
	pingpong  chan bool
	outbound  chan []byte
	close     chan struct{}
}

func NewDrillClient(opts Options) Client {
	return &drillClient{opts: opts}
}

func NewDrillClientWithZK(opts Options, zk ...string) Client {
	return &drillClient{opts: opts, zkNodes: zk}
}

func (d *drillClient) recvRoutine() {
	type readData struct {
		msg *rpc.CompleteRpcMessage
		err error
	}

	inbound := make(chan readData)
	go func() {
		for {
			msg, err := readPrefixedRaw(d.conn)
			if err != nil {
				inbound <- readData{err: err}
				break
			}
			inbound <- readData{msg: msg}
		}
	}()

	defer func() {
		close(d.pingpong)
		d.queryMap.Range(func(_, val interface{}) bool {
			close(val.(chan *rpc.CompleteRpcMessage))
			return true
		})
		d.resultMap.Range(func(_, val interface{}) bool {
			close(val.(chan *queryData))
			return true
		})
	}()

	if d.opts.HeartbeatFreq == nil {
		d.opts.HeartbeatFreq = new(time.Duration)
		*d.opts.HeartbeatFreq = defaultHeartbeatFreq
	}

	if d.opts.HeartbeatFreq != nil && d.opts.HeartbeatFreq.Seconds() > 0 {
		var heartBeatCtx context.Context
		heartBeatCtx, d.cancelHeartBeat = context.WithCancel(context.Background())
		go func() {
			ticker := time.NewTicker(*d.opts.HeartbeatFreq)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if d.Ping(heartBeatCtx) != nil {
						return
					}
				case <-heartBeatCtx.Done():
					return
				}
			}
		}()
	}

	for {
		select {
		case <-d.close:
			return
		case encoded := <-d.outbound:
			_, err := d.conn.Write(makePrefixedMessage(encoded))
			if err != nil {
				// do something
			}
		case data := <-inbound:
			if data.err != nil {
				d.Close()
				return
			}

			if data.msg.Header.GetMode() == rpc.RpcMode_PONG {
				d.pingpong <- true
			}

			switch data.msg.GetHeader().GetRpcType() {
			case int32(user.RpcType_ACK):
				continue
			case int32(user.RpcType_QUERY_HANDLE):
				c, ok := d.queryMap.Load(data.msg.Header.GetCoordinationId())
				if !ok {
					// do something with error
				}

				c.(chan *rpc.CompleteRpcMessage) <- data.msg
			case int32(user.RpcType_QUERY_DATA):
				d.passQueryResponse(data.msg, &shared.QueryData{})
			case int32(user.RpcType_QUERY_RESULT):
				d.passQueryResponse(data.msg, &shared.QueryResult{})
			}
		}
	}
}

func (d *drillClient) passQueryResponse(data *rpc.CompleteRpcMessage, msg proto.Message) {
	d.sendAck(data.Header.GetCoordinationId(), true)
	if err := proto.Unmarshal(data.ProtobufBody, msg); err != nil {
		// do something
	}

	qidField := msg.ProtoReflect().Descriptor().Fields().ByName("query_id")
	q := msg.ProtoReflect().Get(qidField).Message().Interface().(*shared.QueryId)

	c, _ := d.resultMap.Load(qid{q.GetPart1(), q.GetPart2()})
	c.(chan *queryData) <- &queryData{data.Header.GetRpcType(), msg, data.GetRawBody()}
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

	d.outbound <- encoded
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

	d.outbound <- encoded
}

func (d *drillClient) Close() error {
	if d.close != nil {
		close(d.close)
		d.close = nil
	}
	if d.cancelHeartBeat != nil {
		d.cancelHeartBeat()
		d.cancelHeartBeat = nil
	}
	d.conn.Close()
	return nil
}

func (d *drillClient) ConnectEndpoint(ctx context.Context, e Drillbit) error {
	d.endpoint = e
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.endpoint.GetAddress()+":"+strconv.Itoa(int(d.endpoint.GetUserPort())))
	if err != nil {
		return err
	}
	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetNoDelay(true)

	d.conn = conn
	d.coordID = int32(rand.Int()%1729 + 1)
	if err = d.doHandshake(); err != nil {
		return err
	}

	d.close = make(chan struct{})
	d.outbound = make(chan []byte, 10)
	d.pingpong = make(chan bool)

	go d.recvRoutine()

	return nil
}

func (d *drillClient) Connect(ctx context.Context) error {
	zoo, err := NewZKHandler(d.opts.ClusterName, d.zkNodes...)
	if err != nil {
		return err
	}
	defer zoo.Close()

	d.drillBits = zoo.GetDrillBits()
	rand.Shuffle(len(d.drillBits), func(i, j int) {
		d.drillBits[i], d.drillBits[j] = d.drillBits[j], d.drillBits[i]
	})

	d.curBit = 0
	return d.ConnectEndpoint(ctx, zoo.GetEndpoint(d.drillBits[d.curBit]))
}

func (d *drillClient) ConnectWithZK(ctx context.Context, zkNode ...string) error {
	d.zkNodes = zkNode
	return d.Connect(ctx)
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

func (d *drillClient) PrepareQuery(plan string) (driver.Stmt, error) {
	coord := d.nextCoordID()
	req := &user.CreatePreparedStatementReq{SqlQuery: &plan}
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_CREATE_PREPARED_STATEMENT, coord, req)
	if err != nil {
		return nil, err
	}

	queryHandle := make(chan *rpc.CompleteRpcMessage)
	d.queryMap.Store(coord, queryHandle)

	d.outbound <- encoded
	rsp, ok := <-queryHandle
	if !ok || rsp == nil {
		return nil, errors.New("failed to read")
	}

	resp := &user.CreatePreparedStatementResp{}
	if err = proto.Unmarshal(rsp.GetProtobufBody(), resp); err != nil {
		return nil, err
	}

	if resp.GetStatus() != user.RequestStatus_OK {
		return nil, fmt.Errorf("got error: %s", resp.GetError().GetMessage())
	}

	return &prepared{stmt: resp.PreparedStatement, client: d}, nil
}

func (d *drillClient) SubmitQuery(t shared.QueryType, plan string) (*ResultHandle, error) {
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

	queryHandle := make(chan *rpc.CompleteRpcMessage)
	d.queryMap.Store(coord, queryHandle)

	d.outbound <- encoded
	rsp, ok := <-queryHandle
	if !ok || rsp == nil {
		return nil, errors.New("failed to read")
	}

	resp := &shared.QueryId{}
	err = proto.Unmarshal(rsp.ProtobufBody, resp)

	if err != nil {
		return nil, err
	}

	dataChannel := make(chan *queryData, 5)
	d.resultMap.Store(qid{resp.GetPart1(), resp.GetPart2()}, dataChannel)

	return &ResultHandle{
		dataChannel: dataChannel,
		queryID:     resp,
		client:      d,
	}, nil
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

	_, err = d.conn.Write(makePrefixedMessage(encoded))
	if err != nil {
		return err
	}

	d.serverInfo = &user.BitToUserHandshake{}
	_, err = readPrefixedMessage(d.conn, d.serverInfo)
	if err != nil {
		return err
	}

	if d.opts.SaslEncrypt != d.serverInfo.GetEncrypted() {
		return errors.New("invalid security options")
	}

	log.Println("AuthMechs: ", d.serverInfo.GetAuthenticationMechanisms())

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

func (d *drillClient) ExecuteStmt(stmt *user.PreparedStatement) (*ResultHandle, error) {
	coord := d.nextCoordID()
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_REQUEST, user.RpcType_RUN_QUERY, coord, &user.RunQuery{
		ResultsMode:             user.QueryResultsMode_STREAM_FULL.Enum(),
		Type:                    shared.QueryType_PREPARED_STATEMENT.Enum(),
		PreparedStatementHandle: stmt.ServerHandle,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	queryHandle := make(chan *rpc.CompleteRpcMessage)
	d.queryMap.Store(coord, queryHandle)
	d.outbound <- encoded
	rsp, ok := <-queryHandle
	if !ok || rsp == nil {
		return nil, errors.New("failed to read")
	}

	resp := &shared.QueryId{}
	err = proto.Unmarshal(rsp.GetProtobufBody(), resp)
	if err != nil {
		return nil, err
	}

	dataChannel := make(chan *queryData, 5)
	d.resultMap.Store(qid{resp.GetPart1(), resp.GetPart2()}, dataChannel)

	return &ResultHandle{
		dataChannel: dataChannel,
		queryID:     resp,
		client:      d,
	}, nil
}

func (d *drillClient) Ping(ctx context.Context) error {
	coord := d.nextCoordID()
	encoded, err := data.EncodeRpcMessage(rpc.RpcMode_PING, user.RpcType_ACK, coord, &rpc.Ack{Ok: proto.Bool(true)})
	if err != nil {
		return driver.ErrBadConn
	}

	d.outbound <- encoded
	select {
	case val, ok := <-d.pingpong:
		if !ok || !val {
			return driver.ErrBadConn
		}
	case <-ctx.Done():
		return driver.ErrBadConn
	}

	return nil
}
