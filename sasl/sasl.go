package sasl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jcmturner/gokrb5/v8/gssapi"
)

type SaslSecurityProps struct {
	MinSsf        uint32
	MaxSsf        uint32
	MaxBufSize    int32
	UseEncryption bool
}

const (
	saslAuthInit = iota
	saslAuthNeg
	saslAuthSsf
	saslAuthComplete
)

type saslwrapper struct {
	Props SaslSecurityProps
	mech  gssapi.Mechanism
	ct    gssapi.ContextToken

	state byte
}

func NewSaslWrapper(userSpn, serviceSpn string, props SaslSecurityProps) (*saslwrapper, error) {
	krbClient, err := getKrbClient(userSpn)
	if err != nil {
		return nil, err
	}

	return &saslwrapper{
		Props: props,
		mech:  NewGSSAPIKrb5Mech(krbClient, serviceSpn, props),
		state: saslAuthInit,
	}, nil
}

func (s *saslwrapper) InitAuthPayload() ([]byte, error) {
	var err error
	if s.state != saslAuthInit {
		return nil, fmt.Errorf("invalid sasl auth state")
	}

	s.ct, err = s.mech.InitSecContext()
	if err != nil {
		return nil, err
	}

	s.state = saslAuthNeg
	return s.ct.Marshal()
}

func (s *saslwrapper) chooseQop(mechSsf uint32, serverBitMask byte) (uint32, byte) {
	qop := QopNone
	if s.Props.MaxSsf > 0 {
		qop |= QopIntegrity
	}
	if s.Props.MaxSsf > 1 {
		qop |= QopConf
	}

	var allowed uint32 = s.Props.MaxSsf
	var need uint32 = s.Props.MinSsf

	if qop&QopConf != 0 && allowed >= mechSsf && need <= mechSsf && serverBitMask&QopConf != 0 {
		if serverBitMask&QopIntegrity != 0 {
			return mechSsf, QopIntegrity | QopConf
		}
		return mechSsf, QopConf
	} else if qop&QopIntegrity != 0 && allowed >= 1 && need <= 1 && serverBitMask&QopIntegrity != 0 {
		return 1, QopIntegrity
	} else if qop&QopNone != 0 && need <= 0 && serverBitMask&QopNone != 0 {
		return 0, QopNone
	} else {
		return 0, 0
	}
}

func (s *saslwrapper) Step(b []byte) ([]byte, gssapi.Status) {
	switch s.state {
	case saslAuthNeg:
		if err := s.ct.Unmarshal(b); err != nil {
			return nil, gssapi.Status{Code: gssapi.StatusDefectiveCredential, Message: err.Error()}
		}

		s.state = saslAuthSsf

		_, st := s.ct.Verify()
		return nil, st
	case saslAuthSsf:
		var nntoken gssapi.WrapToken
		if err := nntoken.Unmarshal(b, true); err != nil {
			return nil, gssapi.Status{Code: gssapi.StatusDefectiveToken, Message: err.Error()}
		}

		if err := VerifyWrapToken(s.ct, nntoken); err != nil {
			return nil, gssapi.Status{Code: gssapi.StatusBadSig, Message: err.Error()}
		}

		unwrapped := s.mech.Unwrap(nntoken)
		if len(unwrapped) != 4 {
			return nil, gssapi.Status{Code: gssapi.StatusDefectiveToken, Message: fmt.Sprintf("token invalid, should be 4 bytes, not %+v", len(unwrapped))}
		}

		mechSsf := GetSsf(s.ct)
		if s.Props.MinSsf > mechSsf {
			return nil, gssapi.Status{Code: gssapi.StatusBadMech, Message: "sasl too weak"}
		} else if s.Props.MinSsf > s.Props.MaxSsf {
			return nil, gssapi.Status{Code: gssapi.StatusBadMech, Message: "sasl bad param"}
		}

		var qop byte
		mechSsf, qop = s.chooseQop(mechSsf, unwrapped[0])

		maxOutBuf := CalcMaxOutputSize(mechSsf, binary.BigEndian.Uint32(append([]byte{0x00}, unwrapped[1:]...)), s.ct)
		s.Props.MaxBufSize = int32(maxOutBuf)

		out := nntoken.Payload
		binary.BigEndian.PutUint32(out, maxOutBuf)
		out[0] = qop

		token := s.mech.Wrap(out)
		SetQOP(s.ct, qop)
		s.state = saslAuthComplete
		data, err := token.Marshal()
		if err != nil {
			return nil, gssapi.Status{Code: gssapi.StatusDefectiveToken, Message: err.Error()}
		}

		return data, gssapi.Status{Code: gssapi.StatusComplete}
	default:
		return nil, gssapi.Status{Code: gssapi.StatusBadStatus}
	}
}

func (s *saslwrapper) GetWrappedConn(conn net.Conn) net.Conn {
	return &gssapiWrappedConn{
		conn: conn,
		sasl: s,
	}
}

type gssapiWrappedConn struct {
	conn net.Conn
	sasl *saslwrapper

	readBuf bytes.Buffer
}

func (g *gssapiWrappedConn) Close() error {
	return g.conn.Close()
}

func (g *gssapiWrappedConn) LocalAddr() net.Addr {
	return g.conn.LocalAddr()
}

func (g *gssapiWrappedConn) RemoteAddr() net.Addr {
	return g.conn.RemoteAddr()
}

func (g *gssapiWrappedConn) SetDeadline(t time.Time) error {
	return g.conn.SetDeadline(t)
}

func (g *gssapiWrappedConn) SetReadDeadline(t time.Time) error {
	return g.conn.SetReadDeadline(t)
}

func (g *gssapiWrappedConn) SetWriteDeadline(t time.Time) error {
	return g.conn.SetWriteDeadline(t)
}

func (g *gssapiWrappedConn) Read(b []byte) (int, error) {
	n, err := g.readBuf.Read(b)
	if len(b) == n || (err != nil && err != io.EOF) {
		return n, err
	}

	var sz uint32
	if err := binary.Read(g.conn, binary.BigEndian, &sz); err != nil {
		return n, err
	}

	g.readBuf.Reset()
	g.readBuf.Grow(int(sz))
	_, err = io.CopyN(&g.readBuf, g.conn, int64(sz))
	if err != nil {
		return n, err
	}

	var token gssapi.WrapToken
	if err = token.Unmarshal(g.readBuf.Bytes(), true); err != nil {
		return n, err
	}

	g.readBuf.Reset()
	g.readBuf.Write(g.sasl.mech.Unwrap(token))

	return g.readBuf.Read(b)
}

func (g *gssapiWrappedConn) Write(b []byte) (int, error) {
	token := g.sasl.mech.Wrap(b)
	data, err := token.Marshal()
	if err != nil {
		return 0, err
	}

	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	return g.conn.Write(append(hdr, data...))
}
