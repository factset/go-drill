package drill

import (
	"errors"
	"fmt"
	"math"

	"github.com/factset/go-drill/internal/rpc/proto/exec/rpc"
	"github.com/factset/go-drill/internal/rpc/proto/exec/shared"
	"github.com/factset/go-drill/internal/rpc/proto/exec/user"
	"github.com/factset/go-drill/sasl"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"google.golang.org/protobuf/proto"
)

func (d *Client) doHandshake() error {
	u2b := user.UserToBitHandshake{
		Channel:             shared.RpcChannel_USER.Enum(),
		RpcVersion:          proto.Int32(drillRPCVersion),
		SupportListening:    proto.Bool(true),
		SupportTimeout:      proto.Bool(true),
		SaslSupport:         user.SaslSupport_SASL_PRIVACY.Enum(),
		SupportComplexTypes: proto.Bool(d.Opts.SupportComplexTypes),
		ClientInfos: &user.RpcEndpointInfos{
			Name:         proto.String(clientName),
			Version:      proto.String(drillVersion),
			Application:  &d.Opts.ApplicationName,
			MajorVersion: proto.Uint32(drillMajorVersion),
			MinorVersion: proto.Uint32(drillMinorVersion),
			PatchVersion: proto.Uint32(drillPatchVersion),
		},
		Credentials: &shared.UserCredentials{
			UserName: &d.Opts.User,
		},
		Properties: &user.UserProperties{
			Properties: []*user.Property{
				{Key: proto.String("schema"), Value: &d.Opts.Schema},
				{Key: proto.String("userName"), Value: &d.Opts.User},
			},
		},
	}

	if d.Opts.Passwd != "" && (d.Opts.Auth == "PLAIN" || d.Opts.Auth == "") {
		u2b.Properties.Properties = append(u2b.Properties.Properties, &user.Property{Key: proto.String("password"), Value: &d.Opts.Passwd})
	}

	_, err := d.dataEncoder.Write(d.conn, rpc.RpcMode_REQUEST, user.RpcType_HANDSHAKE, d.nextCoordID(), &u2b)
	if err != nil {
		return err
	}

	d.serverInfo = &user.BitToUserHandshake{}
	_, err = d.dataEncoder.ReadMsg(d.conn, d.serverInfo)
	if err != nil {
		return err
	}

	if d.Opts.SaslEncrypt != d.serverInfo.GetEncrypted() {
		return errors.New("invalid security options")
	}

	switch d.serverInfo.GetStatus() {
	case user.HandshakeStatus_SUCCESS:
		if (len(d.Opts.Auth) > 0 && d.Opts.Auth != "plain") || d.Opts.SaslEncrypt {
			return errors.New("client wanted auth, but server didn't require it")
		}
	case user.HandshakeStatus_RPC_VERSION_MISMATCH:
		return fmt.Errorf("invalid rpc version, expected: %d, actual: %d", drillRPCVersion, d.serverInfo.GetRpcVersion())
	case user.HandshakeStatus_AUTH_FAILED:
		return errors.New("authentication failure")
	case user.HandshakeStatus_UNKNOWN_FAILURE:
		return errors.New("unknown handshake failure")
	case user.HandshakeStatus_AUTH_REQUIRED:
		return d.handleAuth()
	}

	return nil
}

var createSasl = sasl.NewSaslWrapper

func (d *Client) handleAuth() error {
	if ((len(d.Opts.Auth) > 0 && d.Opts.Auth != "plain") || d.Opts.SaslEncrypt) && !d.serverInfo.GetEncrypted() {
		return errors.New("client wants encryption, server doesn't support encryption")
	}

	host := d.Opts.ServiceHost
	if d.Opts.ServiceHost == "_HOST" || d.Opts.ServiceHost == "" {
		host = d.endpoint.GetAddress()
	}

	wrapper, err := createSasl(d.Opts.User, d.Opts.ServiceName+"/"+host, sasl.SecurityProps{
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

	d.dataEncoder.Write(d.conn, rpc.RpcMode_REQUEST, user.RpcType_SASL_MESSAGE, d.nextCoordID(), &shared.SaslMessage{
		Mechanism: &d.Opts.Auth,
		Data:      token,
		Status:    shared.SaslStatus_SASL_START.Enum(),
	})

	saslResp := &shared.SaslMessage{}
	_, err = d.dataEncoder.ReadMsg(d.conn, saslResp)
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

		d.dataEncoder.Write(d.conn, rpc.RpcMode_REQUEST, user.RpcType_SASL_MESSAGE, d.nextCoordID(), &shared.SaslMessage{
			Data:   token,
			Status: encodeStatus,
		})

		_, err = d.dataEncoder.ReadMsg(d.conn, saslResp)
		if err != nil {
			return err
		}
	}

	d.conn = wrapper.GetWrappedConn(d.conn)

	return nil
}
