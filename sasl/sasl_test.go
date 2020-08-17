package sasl

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var testkey types.EncryptionKey
var testAuthCtx authContext

func init() {
	testkey, _ = types.GenerateEncryptionKey(crypto.Aes128CtsHmacSha96{})
	testAuthCtx.key = testkey
}

type mockToken struct {
	tomarshal []byte
	err       error
	verify    bool
	st        gssapi.Status
}

func (m *mockToken) Marshal() ([]byte, error) { return m.tomarshal, m.err }
func (m *mockToken) Unmarshal(b []byte) error {
	m.tomarshal = b
	return m.err
}
func (m *mockToken) Verify() (bool, gssapi.Status) {
	return m.verify, m.st
}
func (m *mockToken) Context() context.Context {
	return context.WithValue(context.TODO(), ctxAuthCtx, &testAuthCtx)
}

type mockMech struct {
	mock.Mock
}

func (m *mockMech) OID() asn1.ObjectIdentifier              { return gssapi.OIDGSSIAKerb.OID() }
func (m *mockMech) AcquireCred() error                      { return nil }
func (m *mockMech) MIC() gssapi.MICToken                    { return gssapi.MICToken{} }
func (m *mockMech) VerifyMIC(gssapi.MICToken) (bool, error) { return false, nil }
func (m *mockMech) Wrap(msg []byte) gssapi.WrapToken {
	return m.Called(msg).Get(0).(gssapi.WrapToken)
}
func (m *mockMech) Unwrap(wt gssapi.WrapToken) []byte {
	return m.Called(wt).Get(0).([]byte)
}

func (m *mockMech) AcceptSecContext(gssapi.ContextToken) (bool, context.Context, gssapi.Status) {
	return false, nil, gssapi.Status{}
}

func (m *mockMech) InitSecContext() (gssapi.ContextToken, error) {
	args := m.Called()
	return args.Get(0).(gssapi.ContextToken), args.Error(1)
}

var deadbeef = []byte{0xde, 0xad, 0xbe, 0xef}

func TestInitAuth(t *testing.T) {
	m := new(mockMech)
	m.Test(t)
	defer m.AssertExpectations(t)

	token := &mockToken{tomarshal: deadbeef}

	m.On("InitSecContext").Return(token, nil)

	s := &saslwrapper{
		state: saslAuthInit,
		mech:  m,
	}

	data, err := s.InitAuthPayload()
	assert.NoError(t, err)
	assert.Equal(t, deadbeef, data)

	_, err = s.InitAuthPayload()
	assert.EqualError(t, err, "invalid sasl auth state")
}

func TestInitAuthSecFail(t *testing.T) {
	m := new(mockMech)
	m.Test(t)
	defer m.AssertExpectations(t)

	m.On("InitSecContext").Return((*mockToken)(nil), assert.AnError)

	s := &saslwrapper{
		state: saslAuthInit,
		mech:  m,
	}

	data, err := s.InitAuthPayload()
	assert.Nil(t, data)
	assert.Same(t, assert.AnError, err)
}

func TestStepAuthNeg(t *testing.T) {
	token := &mockToken{st: gssapi.Status{Code: gssapi.StatusComplete}}
	s := &saslwrapper{
		state: saslAuthNeg,
		ct:    token,
	}

	data, st := s.Step(deadbeef)
	assert.Nil(t, data)
	assert.Equal(t, gssapi.Status{Code: gssapi.StatusComplete}, st)

	assert.Equal(t, deadbeef, token.tomarshal)
	assert.Equal(t, byte(saslAuthSsf), s.state)
}

func TestStepAuthNegUnmarshalErr(t *testing.T) {
	token := &mockToken{err: assert.AnError}
	s := &saslwrapper{
		state: saslAuthNeg,
		ct:    token,
	}

	_, st := s.Step(deadbeef)
	assert.Equal(t, assert.AnError.Error(), st.Message)
}

func TestAuthSSFTokenFail(t *testing.T) {
	s := &saslwrapper{
		state: saslAuthSsf,
	}

	val, st := s.Step(deadbeef)
	assert.Nil(t, val)
	assert.Equal(t, gssapi.StatusDefectiveToken, st.Code)
}

func TestAuthSSFTokenFailVerify(t *testing.T) {
	s := &saslwrapper{
		state: saslAuthSsf,
		ct:    &mockToken{},
	}

	token, _ := gssapi.NewInitiatorWrapToken(deadbeef, testkey)
	token.Flags |= 0x01
	tokenbytes, _ := token.Marshal()
	val, st := s.Step(tokenbytes)
	assert.Nil(t, val)
	assert.Equal(t, gssapi.StatusBadSig, st.Code)
}

func TestAuthSSFPayloadTooBig(t *testing.T) {
	m := new(mockMech)
	m.Test(t)
	defer m.AssertExpectations(t)

	token := &mockToken{}
	s := &saslwrapper{
		mech:  m,
		state: saslAuthSsf,
		ct:    token,
	}

	etyp, _ := crypto.GetEtype(testkey.KeyType)
	wraptoken := gssapi.WrapToken{
		Flags:   0x01,
		EC:      uint16(etyp.GetHMACBitLength() / 8),
		Payload: []byte{0x01, 0x01, 0x01, 0x01, 0x01},
	}

	assert.NoError(t, wraptoken.SetCheckSum(testkey, keyusage.GSSAPI_ACCEPTOR_SEAL))

	m.On("Unwrap", wraptoken).Return(wraptoken.Payload)

	tokenbytes, _ := wraptoken.Marshal()
	val, st := s.Step(tokenbytes)
	assert.Nil(t, val)
	assert.Equal(t, "token invalid, should be 4 bytes, not 5", st.Message)
}

func TestAuthSSFTooWeak(t *testing.T) {
	m := new(mockMech)
	m.Test(t)
	defer m.AssertExpectations(t)

	token := &mockToken{}
	s := &saslwrapper{
		mech:  m,
		state: saslAuthSsf,
		ct:    token,
		Props: SecurityProps{
			MinSsf: 256,
		},
	}

	etyp, _ := crypto.GetEtype(testkey.KeyType)
	wraptoken := gssapi.WrapToken{
		Flags:   0x01,
		EC:      uint16(etyp.GetHMACBitLength() / 8),
		Payload: []byte{0x01, 0x01, 0x01, 0x01},
	}

	assert.NoError(t, wraptoken.SetCheckSum(testkey, keyusage.GSSAPI_ACCEPTOR_SEAL))

	m.On("Unwrap", wraptoken).Return(wraptoken.Payload)

	tokenbytes, _ := wraptoken.Marshal()
	val, st := s.Step(tokenbytes)
	assert.Nil(t, val)
	assert.Equal(t, "sasl too weak", st.Message)
}

func TestAuthSSFSaslBad(t *testing.T) {
	m := new(mockMech)
	m.Test(t)
	defer m.AssertExpectations(t)

	token := &mockToken{}
	s := &saslwrapper{
		mech:  m,
		state: saslAuthSsf,
		ct:    token,
		Props: SecurityProps{
			MinSsf: 100,
			MaxSsf: 0,
		},
	}

	etyp, _ := crypto.GetEtype(testkey.KeyType)
	wraptoken := gssapi.WrapToken{
		Flags:   0x01,
		EC:      uint16(etyp.GetHMACBitLength() / 8),
		Payload: []byte{0x01, 0x01, 0x01, 0x01},
	}

	assert.NoError(t, wraptoken.SetCheckSum(testkey, keyusage.GSSAPI_ACCEPTOR_SEAL))

	m.On("Unwrap", wraptoken).Return(wraptoken.Payload)

	tokenbytes, _ := wraptoken.Marshal()
	val, st := s.Step(tokenbytes)
	assert.Nil(t, val)
	assert.Equal(t, "sasl bad param", st.Message)
}

func TestAuthSsfBadWrap(t *testing.T) {
	m := new(mockMech)
	m.Test(t)
	defer m.AssertExpectations(t)

	token := &mockToken{}
	s := &saslwrapper{
		mech:  m,
		state: saslAuthSsf,
		ct:    token,
	}

	etyp, _ := crypto.GetEtype(testkey.KeyType)
	wraptoken := gssapi.WrapToken{
		Flags:   0x01,
		EC:      uint16(etyp.GetHMACBitLength() / 8),
		Payload: []byte{0x01, 0x01, 0x01, 0x01},
	}

	assert.NoError(t, wraptoken.SetCheckSum(testkey, keyusage.GSSAPI_ACCEPTOR_SEAL))

	m.On("Wrap", []byte{0x01, 0x01, 0x01, 0x01}).Return(gssapi.WrapToken{})
	m.On("Unwrap", wraptoken).Return(wraptoken.Payload)

	tokenbytes, _ := wraptoken.Marshal()
	val, st := s.Step(tokenbytes)
	assert.Nil(t, val)
	assert.Equal(t, gssapi.StatusDefectiveToken, st.Code)
}

func TestAuthSSF(t *testing.T) {
	m := new(mockMech)
	m.Test(t)
	defer m.AssertExpectations(t)

	token := &mockToken{}
	s := &saslwrapper{
		mech:  m,
		state: saslAuthSsf,
		ct:    token,
		Props: SecurityProps{
			MaxSsf: 256,
		},
	}

	etyp, _ := crypto.GetEtype(testkey.KeyType)
	wraptoken := gssapi.WrapToken{
		Flags:   0x01,
		EC:      uint16(etyp.GetHMACBitLength() / 8),
		Payload: []byte{0x02, 0x01, 0x00, 0xc5},
	}

	outtoken := gssapi.WrapToken{
		Flags:   0x00,
		EC:      uint16(etyp.GetHMACBitLength() / 8),
		Payload: []byte{0x02, 0x01, 0x00, 0x89},
	}

	assert.NoError(t, outtoken.SetCheckSum(testkey, keyusage.GSSAPI_INITIATOR_SEAL))
	assert.NoError(t, wraptoken.SetCheckSum(testkey, keyusage.GSSAPI_ACCEPTOR_SEAL))

	m.On("Wrap", []byte{0x02, 0x01, 0x0, 0x89}).Return(outtoken)
	m.On("Unwrap", wraptoken).Return(wraptoken.Payload)

	tokenbytes, _ := wraptoken.Marshal()
	outbytes, _ := outtoken.Marshal()

	val, st := s.Step(tokenbytes)
	assert.Equal(t, outbytes, val)
	assert.Equal(t, gssapi.Status{Code: gssapi.StatusComplete}, st)

	val, st = s.Step(tokenbytes)
	assert.Nil(t, val)
	assert.Equal(t, gssapi.StatusBadStatus, st.Code)
}

func TestChooseQOPConf(t *testing.T) {
	s := &saslwrapper{
		Props: SecurityProps{
			MaxSsf: 256,
		},
	}

	ssf, q := s.chooseQop(128, QopConf)
	assert.Equal(t, uint32(128), ssf)
	assert.Equal(t, QopConf, q)
}

func TestChooseQOPConfInteg(t *testing.T) {
	s := &saslwrapper{
		Props: SecurityProps{
			MaxSsf: 256,
		},
	}

	ssf, q := s.chooseQop(128, QopConf|QopIntegrity|QopNone)
	assert.Equal(t, uint32(128), ssf)
	assert.Equal(t, QopConf|QopIntegrity, q)
}

type mockConn struct {
	r io.Reader
	mock.Mock
}

type mockAddr struct{}

func (mockAddr) Network() string { return "foo" }
func (mockAddr) String() string  { return "bar" }

func (m *mockConn) Close() error                       { return m.Called().Error(0) }
func (m *mockConn) LocalAddr() net.Addr                { return m.Called().Get(0).(mockAddr) }
func (m *mockConn) RemoteAddr() net.Addr               { return m.Called().Get(0).(mockAddr) }
func (m *mockConn) SetDeadline(t time.Time) error      { return m.Called(t).Error(0) }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return m.Called(t).Error(0) }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return m.Called(t).Error(0) }

func (m *mockConn) Read(b []byte) (int, error) {
	m.Called()
	return m.r.Read(b)
}

func (m *mockConn) Write(b []byte) (int, error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

func TestBaseWrappedConn(t *testing.T) {
	s := &saslwrapper{}

	mc := new(mockConn)
	mc.Test(t)
	defer mc.AssertExpectations(t)

	mc.On("Close").Return(assert.AnError).Once()
	assert.Same(t, assert.AnError, s.GetWrappedConn(mc).Close())

	ma := mockAddr{}
	mc.On("LocalAddr").Return(ma).Once()
	assert.Equal(t, ma, s.GetWrappedConn(mc).LocalAddr())
	mc.On("RemoteAddr").Return(ma).Once()
	assert.Equal(t, ma, s.GetWrappedConn(mc).RemoteAddr())

	testT := time.Now()
	mc.On("SetDeadline", testT).Return(assert.AnError).Once()
	assert.Same(t, assert.AnError, s.GetWrappedConn(mc).SetDeadline(testT))
	mc.On("SetReadDeadline", testT).Return(assert.AnError).Once()
	assert.Same(t, assert.AnError, s.GetWrappedConn(mc).SetReadDeadline(testT))
	mc.On("SetWriteDeadline", testT).Return(assert.AnError).Once()
	assert.Same(t, assert.AnError, s.GetWrappedConn(mc).SetWriteDeadline(testT))
}
