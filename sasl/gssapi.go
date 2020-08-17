package sasl

import (
	"context"
	"math"
	"math/rand"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/jcmturner/gokrb5/v8/types"
)

// based on krb5_gssapi_encrypt_length
func getEncryptSize(key types.EncryptionKey, len uint32) uint32 {
	etyp, _ := crypto.GetEtype(key.KeyType)
	return uint32(etyp.GetConfounderByteSize()+etyp.GetHMACBitLength()/8) + len
}

func genSeqNumber() uint64 {
	//  Work around implementation incompatibilities by not generating
	//  initial sequence numbers greater than 2^30.  Previous MIT
	//  implementations use signed sequence numbers, so initial
	//  sequence numbers 2^31 to 2^32-1 inclusive will be rejected.
	//  Letting the maximum initial sequence number be 2^30-1 allows
	//  for about 2^30 messages to be sent before wrapping into
	//  "negative" numbers.
	return uint64(rand.Int63n(int64(math.Pow(2, 30))))
}

// NewGSSAPIKrb5Mech constructs a mechanism for gssapi processing using Kerberos via krb5
func NewGSSAPIKrb5Mech(cl *client.Client, spn string, saslProps SecurityProps) gssapi.Mechanism {
	return &gssapiKrb5Mech{cl: cl, spn: spn, saslProps: saslProps}
}

type gssapiKrb5Mech struct {
	cl  *client.Client
	spn string

	saslProps SecurityProps
	ctx       authContext
}

// Qop is a bitmask representing the current Quality of Protection settings
type Qop byte

// Qop will be some combination of none / integrity / confidential
const (
	QopNone Qop = 1 << iota
	QopIntegrity
	QopConf
)

// opaque context object for internal handling
type authContext struct {
	key          types.EncryptionKey
	remoteSeqNum int64
	subKey       types.EncryptionKey
	qop          Qop
	localSeqNum  uint64
}

// opaque token that fulfills the interface defined in gssapi.ContextToken
type gssapiKrb5Token struct {
	krb5Tok spnego.KRB5Token

	ctx *authContext
}

func (g *gssapiKrb5Token) Marshal() ([]byte, error) {
	return g.krb5Tok.Marshal()
}

func (g *gssapiKrb5Token) Unmarshal(b []byte) error {
	return g.krb5Tok.Unmarshal(b)
}

type contextKey int

const (
	ctxAuthCtx contextKey = iota
)

// VerifyWrapToken allows calling Verify on the token without having to expose
// the encryption key that the context token is holding onto.
func VerifyWrapToken(ct gssapi.ContextToken, wt gssapi.WrapToken) error {
	key := ct.Context().Value(ctxAuthCtx).(*authContext).key
	_, err := wt.Verify(key, keyusage.GSSAPI_ACCEPTOR_SEAL)
	return err
}

// GetSsf uses the opaque context in the token in order to pull the key and return
// the Security Strength Factor (ssf) value for the given key.
func GetSsf(ct gssapi.ContextToken) uint32 {
	key := ct.Context().Value(ctxAuthCtx).(*authContext).key
	etyp, _ := crypto.GetEtype(key.KeyType)
	return uint32(etyp.GetKeySeedBitLength())
}

// CalcMaxOutputSize uses the determined SSF value and provided max buffer size
// combined with the encryption key in the token to figure out what the actual
// max size can be such that the resulting size after encryption will still be
// within the provided maxOutBuf.
//
// As per the general SASL definitions, if the SSF is <= 0, then we wouldn't be
// encrypting the buffer, and just return the maxOutBuf that was passed in. If
// mechSsf > 0, then we grab the key and figure out what size will encrypt to
// a size smaller than the passed in maxOutBuf while also giving room for the 16
// byte token header.
func CalcMaxOutputSize(mechSsf, maxOutBuf uint32, ct gssapi.ContextToken) uint32 {
	if mechSsf > 0 {
		key := ct.Context().Value(ctxAuthCtx).(*authContext).key

		sz := maxOutBuf
		for sz > 0 && getEncryptSize(key, sz+16) > maxOutBuf {
			sz--
		}

		if sz > 0 {
			sz -= 16
		} else {
			sz = 0
		}
		return sz
	}
	return maxOutBuf
}

// SetQOP will set the desired Qop value into the opaque token value
func SetQOP(ct gssapi.ContextToken, qop Qop) {
	ct.Context().Value(ctxAuthCtx).(*authContext).qop = qop
}

func (g *gssapiKrb5Token) Verify() (bool, gssapi.Status) {
	valid, st := g.krb5Tok.Verify()
	if !valid && st.Code == gssapi.StatusFailure {
		// gokrb5 doesn't verify APREP yet, but i can!
		b, err := crypto.DecryptEncPart(g.krb5Tok.APRep.EncPart, g.ctx.key, keyusage.AP_REP_ENCPART)
		if err != nil {
			return false, gssapi.Status{Code: gssapi.StatusDefectiveCredential, Message: "Could not decrypt APRep"}
		}

		var denc messages.EncAPRepPart
		if err = denc.Unmarshal(b); err != nil {
			return false, gssapi.Status{Code: gssapi.StatusFailure, Message: err.Error()}
		}

		g.ctx.remoteSeqNum = denc.SequenceNumber
		g.ctx.subKey = denc.Subkey
		// TODO: use denc.CTime and denc.Cusec to verify no clock skew
		return true, gssapi.Status{Code: gssapi.StatusContinueNeeded}
	}
	return valid, st
}

// Context will return a context.Context that also contains the opaque auth context
// embedded in it so that it can be used and passed around
func (g *gssapiKrb5Token) Context() context.Context {
	return context.WithValue(g.krb5Tok.Context(), ctxAuthCtx, g.ctx)
}

// getCtxFlags will provide the list of flags to pass to gssapi creation
// based on the sasl props to determine whether or not we want to use
// integrity checking and/or confidentiality
func (g *gssapiKrb5Mech) getCtxFlags() []int {
	ret := []int{gssapi.ContextFlagMutual, gssapi.ContextFlagSequence}
	if g.saslProps.UseEncryption {
		ret = append(ret, gssapi.ContextFlagConf, gssapi.ContextFlagInteg)
		return ret
	}

	if g.saslProps.MaxSsf > 0 {
		ret = append(ret, gssapi.ContextFlagInteg)
		if g.saslProps.MaxSsf > 1 {
			ret = append(ret, gssapi.ContextFlagConf)
		}
	}
	return ret
}

func (gssapiKrb5Mech) OID() asn1.ObjectIdentifier {
	return gssapi.OIDKRB5.OID()
}

func (g *gssapiKrb5Mech) AcquireCred() error {
	return g.cl.AffirmLogin()
}

// AcceptSecContext currently is unimplemented beyond calling Verify on the token
// this does not yet set up the local security context appropriately
func (g *gssapiKrb5Mech) AcceptSecContext(ct gssapi.ContextToken) (bool, context.Context, gssapi.Status) {
	valid, st := ct.Verify()
	return valid, ct.Context(), st
}

// InitSecContext uses the spn we initialized with to perform Krb initialization
// by grabbing the service ticket and doing an AP Exchange
func (g *gssapiKrb5Mech) InitSecContext() (gssapi.ContextToken, error) {
	ticket, key, err := g.cl.GetServiceTicket(g.spn)
	if err != nil {
		return nil, err
	}

	g.ctx.key = key

	tok, err := spnego.NewKRB5TokenAPREQ(g.cl, ticket, key, g.getCtxFlags(), []int{})
	if err != nil {
		return nil, err
	}

	g.ctx.localSeqNum = genSeqNumber()

	return &gssapiKrb5Token{krb5Tok: tok, ctx: &g.ctx}, nil
}

// MIC tokens are currently unimplemented
func (g *gssapiKrb5Mech) MIC() gssapi.MICToken {
	return gssapi.MICToken{}
}

func (g *gssapiKrb5Mech) VerifyMIC(mt gssapi.MICToken) (bool, error) {
	return mt.Verify(g.ctx.key, keyusage.GSSAPI_ACCEPTOR_SEAL)
}

// Wrap will use the current QOP settings in order to determine whether or not
// we'll actually encrypt the data and then returns the correct wrapped data
func (g *gssapiKrb5Mech) Wrap(msg []byte) gssapi.WrapToken {
	var data []byte

	if (g.ctx.qop & QopConf) != 0 {
		tok := gssapi.WrapToken{
			Flags:     0x02,
			EC:        0,
			RRC:       0,
			SndSeqNum: g.ctx.localSeqNum,
			CheckSum:  make([]byte, 0),
			Payload:   make([]byte, 0),
		}

		hdr, err := tok.Marshal()
		if err != nil {
			panic(err)
		}

		etyp, _ := crypto.GetEtype(g.ctx.key.KeyType)
		_, data, err = etyp.EncryptMessage(g.ctx.key.KeyValue, append(msg, hdr...), keyusage.GSSAPI_INITIATOR_SEAL)
		if err != nil {
			panic(err)
		}

		g.ctx.localSeqNum++

		tok.Payload = data
		return tok
	}

	token, err := gssapi.NewInitiatorWrapToken(msg, g.ctx.key)
	if err != nil {
		panic(err)
	}
	return *token
}

// Unwrap will check the flags to determine whether or not we should decrypt the data
// or just return the unwrapped payload
func (g *gssapiKrb5Mech) Unwrap(wt gssapi.WrapToken) []byte {
	if wt.Flags&0x02 == 0 {
		return wt.Payload
	}

	decoded, err := crypto.DecryptMessage(wt.Payload, g.ctx.key, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if err != nil {
		panic(err)
	}

	return decoded[:len(decoded)-16]
}
