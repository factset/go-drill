package sasl

import (
	"testing"

	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/stretchr/testify/assert"
)

func TestTokenContext(t *testing.T) {
	tk := gssapiKrb5Token{ctx: &authContext{}}

	assert.Same(t, tk.ctx, tk.Context().Value(ctxAuthCtx))
}

func TestGssapiMechOID(t *testing.T) {
	mech := NewGSSAPIKrb5Mech(nil, "foobar", SecurityProps{})
	assert.Equal(t, gssapi.OIDKRB5.OID(), mech.OID())
}

func TestGssapiMIC(t *testing.T) {
	// just returns a blank MICToken for now:
	g := gssapiKrb5Mech{}
	assert.Equal(t, gssapi.MICToken{}, g.MIC())
}

func TestGssapiAcceptSecContext(t *testing.T) {
	sampleSt := gssapi.Status{Code: gssapi.StatusContinueNeeded}
	tk := &mockToken{verify: true, st: sampleSt}

	g := gssapiKrb5Mech{}
	valid, _, st := g.AcceptSecContext(tk)
	assert.True(t, valid)
	assert.Equal(t, sampleSt, st)
}

func TestGssapiMechCtxFlags(t *testing.T) {
	tests := []struct {
		name       string
		props      SecurityProps
		extraFlags []int
	}{
		{"encryption flags", SecurityProps{UseEncryption: true}, []int{gssapi.ContextFlagConf, gssapi.ContextFlagInteg}},
		{"integrity flag", SecurityProps{MaxSsf: 1}, []int{gssapi.ContextFlagInteg}},
		{"conf flag", SecurityProps{MaxSsf: 256}, []int{gssapi.ContextFlagInteg, gssapi.ContextFlagConf}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gssapiKrb5Mech{saslProps: tt.props}
			assert.ElementsMatch(t,
				append([]int{gssapi.ContextFlagMutual /*, gssapi.ContextFlagSequence*/},
					tt.extraFlags...), g.getCtxFlags())
		})
	}
}

func TestGssapiMechUnwrapNoEncrypt(t *testing.T) {
	g := gssapiKrb5Mech{}
	assert.Equal(t, deadbeef, g.Unwrap(gssapi.WrapToken{Payload: deadbeef}))
}

func TestGssapiUnwrapEncrypted(t *testing.T) {
	tokenHdrBytes := []byte{5, 4, 2, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

	etyp, _ := crypto.GetEtype(testkey.KeyType)
	_, data, _ := etyp.EncryptMessage(testkey.KeyValue, append(deadbeef, tokenHdrBytes...), keyusage.GSSAPI_ACCEPTOR_SEAL)

	g := gssapiKrb5Mech{
		ctx: authContext{key: testkey},
	}

	assert.Equal(t, deadbeef, g.Unwrap(gssapi.WrapToken{
		Flags:     0x02,
		EC:        0,
		RRC:       0,
		SndSeqNum: 1,
		CheckSum:  make([]byte, 0),
		Payload:   data,
	}))
}

func TestGssapiUnwrapPanic(t *testing.T) {
	g := gssapiKrb5Mech{
		ctx: authContext{key: testkey},
	}

	assert.Panics(t, func() {
		g.Unwrap(gssapi.WrapToken{
			Flags:   0x02,
			Payload: deadbeef,
		})
	})
}

func TestGssapiWrapNoConf(t *testing.T) {
	g := gssapiKrb5Mech{ctx: authContext{key: testkey}}

	tok := g.Wrap(deadbeef)
	assert.Equal(t, deadbeef, tok.Payload)
}

func TestGssapiWrapNoConfPanic(t *testing.T) {
	g := gssapiKrb5Mech{}
	// panics because we have no key
	assert.Panics(t, func() { g.Wrap(deadbeef) })
}

func TestGssapiWrapEncrypt(t *testing.T) {
	g := gssapiKrb5Mech{ctx: authContext{key: testkey, qop: QopConf, localSeqNum: 1}}

	tok := g.Wrap(deadbeef)

	data, err := crypto.DecryptMessage(tok.Payload, testkey, keyusage.GSSAPI_INITIATOR_SEAL)
	assert.NoError(t, err)
	assert.Equal(t, append(deadbeef, []byte{5, 4, 2, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}...), data)
	assert.EqualValues(t, 2, g.ctx.localSeqNum)
}
