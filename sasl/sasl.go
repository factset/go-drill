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

// SecurityProps simply contains settings used for the sasl negotiation.
//
// These are utilized by the gssapi mechanism in order to determine the QOP settings
type SecurityProps struct {
	MinSsf        uint32
	MaxSsf        uint32
	MaxBufSize    int32
	UseEncryption bool
}

// the current state of our authentication
const (
	saslAuthInit = iota
	saslAuthNeg
	saslAuthSsf
	saslAuthComplete
)

// Wrapper is the primary interface for sasl-gssapi handling.
//
// A wrapper is returned from NewSaslWrapper which will allow performing authentication
// and then wrapping a desired connection to properly wrap and unwrap messages.
type Wrapper interface {
	// InitAuthPayload initializes the local security context and returns a payload
	// for sending the initial token for negotiation.
	InitAuthPayload() ([]byte, error)
	// Step takes the responses from the server (eg. auth challenges) and steps through
	// the authentication and negotiation protocols, returning the next payload response
	// to send to the server as long as the gssapi.Status is gssapi.StatusContinueNeeded.
	// When authentication is complete, the status will be gssapi.StatusComplete. Any other
	// status will come associated with an error
	Step([]byte) ([]byte, gssapi.Status)
	// GetWrappedConn takes the provided connection and wraps it such that anything written
	// to or read from the connection will be put through the wrap/unwrap calls of the
	// sasl authentication based on the negotiated security context.
	GetWrappedConn(net.Conn) net.Conn
}

type saslwrapper struct {
	Props SecurityProps
	mech  gssapi.Mechanism
	ct    gssapi.ContextToken

	state byte
}

// NewSaslWrapper takes the provided SPNs and SecurityProps to provide a Wrapper
// that will perform GSSAPI authentication via kerberos krb5
func NewSaslWrapper(userSpn, serviceSpn string, props SecurityProps) (Wrapper, error) {
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

// chooseQop returns both the Qop bytes and the chosen ssf value
func (s *saslwrapper) chooseQop(mechSsf uint32, serverBitMask Qop) (uint32, Qop) {
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
		// handle response from InitAuthPayload
		if err := s.ct.Unmarshal(b); err != nil {
			return nil, gssapi.Status{Code: gssapi.StatusDefectiveCredential, Message: err.Error()}
		}

		// next step is negotating the ssf value
		s.state = saslAuthSsf

		_, st := s.ct.Verify()
		return nil, st
	case saslAuthSsf:
		var nntoken gssapi.WrapToken
		// our ssf negotiation will not have the payload encrypted
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

		// the 4 bytes we got back should be:
		// 	[0] == server Qop Bitmask
		//  [1-3] == BigEndian encoded uint32 server max buffer size for a single payload

		var qop Qop
		mechSsf, qop = s.chooseQop(mechSsf, Qop(unwrapped[0]))

		maxOutBuf := CalcMaxOutputSize(mechSsf, binary.BigEndian.Uint32(append([]byte{0x00}, unwrapped[1:]...)), s.ct)
		s.Props.MaxBufSize = int32(maxOutBuf)

		// our response should be formatted the same way:
		// replacing the first byte with the desired QOP value
		out := nntoken.Payload
		binary.BigEndian.PutUint32(out, maxOutBuf)
		out[0] = byte(qop)

		token := s.mech.Wrap(out)
		// update our auth context with the chosen qop value *after* we wrap our response
		// since the response should not be encrypted even if our future communications will be
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

// fulfill the net.Conn interface

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
	// use an internal buffer here
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
