// Derived from github.com/livekit/sip (Apache-2.0), DTLS-SRTP/ICE work by mab3321.
package media

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	pdtls "github.com/pion/dtls/v3"
	pice "github.com/pion/ice/v4"
	"github.com/pion/rtp"
	psrtp "github.com/pion/srtp/v3"
)

type Transport struct {
	conf                 *Negotiation
	raw                  *net.UDPConn
	timeout              time.Duration
	ctx                  context.Context
	cancel               context.CancelFunc
	ready                chan struct{}
	startOnce, closeOnce sync.Once
	mu                   sync.RWMutex
	err                  error
	closed               bool
	mux                  *packetMux
	dtls                 *pdtls.Conn
	srtp                 *psrtp.SessionSRTP
	srtcp                *psrtp.SessionSRTCP
	iceAgent             *pice.Agent
	iceMux               pice.UDPMux
	readMu               sync.Mutex
	read                 *psrtp.ReadStreamSRTP
	writeMu              sync.Mutex
	write                *psrtp.WriteStreamSRTP
}

func NewTransport(parent context.Context, raw *net.UDPConn, conf *Negotiation, timeout time.Duration) *Transport {
	ctx, cancel := context.WithCancel(parent)
	return &Transport{conf: conf, raw: raw, timeout: timeout, ctx: ctx, cancel: cancel, ready: make(chan struct{})}
}
func (t *Transport) Start() { t.startOnce.Do(func() { go t.run() }) }
func (t *Transport) run() {
	defer close(t.ready)
	ctx, cancel := context.WithTimeout(t.ctx, t.timeout)
	defer cancel()
	conn, err := t.connectICE(ctx)
	if err != nil {
		t.setResult(nil, nil, nil, nil, err)
		return
	}
	mux := newPacketMux(conn)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = mux.Close()
		return
	}
	t.mux = mux
	t.mu.Unlock()
	verify := func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("DTLS peer sent no certificate")
		}
		d := sha256.Sum256(rawCerts[0])
		p := make([]string, len(d))
		for i, b := range d {
			p[i] = fmt.Sprintf("%02X", b)
		}
		if !strings.EqualFold(strings.Join(p, ":"), t.conf.RemoteFingerprint) {
			return errors.New("DTLS peer fingerprint mismatch")
		}
		return nil
	}
	cfg := &pdtls.Config{Certificates: []tls.Certificate{t.conf.Certificate.certificate}, InsecureSkipVerify: true, VerifyPeerCertificate: verify, ClientAuth: pdtls.RequireAnyClientCert, SRTPProtectionProfiles: []pdtls.SRTPProtectionProfile{pdtls.SRTP_AEAD_AES_128_GCM, pdtls.SRTP_AES128_CM_HMAC_SHA1_80}}
	var dc *pdtls.Conn
	if t.conf.IsClient {
		dc, err = pdtls.Client(mux.dtls, conn.RemoteAddr(), cfg)
	} else {
		dc, err = pdtls.Server(mux.dtls, conn.RemoteAddr(), cfg)
	}
	if err == nil {
		err = dc.HandshakeContext(ctx)
	}
	var sr *psrtp.SessionSRTP
	var sc *psrtp.SessionSRTCP
	if err == nil {
		var ok bool
		var profile pdtls.SRTPProtectionProfile
		profile, ok = dc.SelectedSRTPProtectionProfile()
		if !ok {
			err = errors.New("DTLS did not negotiate SRTP")
		}
		var sp psrtp.ProtectionProfile
		if err == nil {
			switch profile {
			case pdtls.SRTP_AEAD_AES_128_GCM:
				sp = psrtp.ProtectionProfileAeadAes128Gcm
			case pdtls.SRTP_AES128_CM_HMAC_SHA1_80:
				sp = psrtp.ProtectionProfileAes128CmHmacSha1_80
			default:
				err = fmt.Errorf("unsupported SRTP profile %v", profile)
			}
		}
		if err == nil {
			state, ok := dc.ConnectionState()
			if !ok {
				err = errors.New("DTLS state unavailable")
			} else {
				scfg := &psrtp.Config{Profile: sp}
				err = scfg.ExtractSessionKeysFromDTLS(&state, t.conf.IsClient)
				if err == nil {
					sr, err = psrtp.NewSessionSRTP(mux.srtp, scfg)
				}
				if err == nil {
					sc, err = psrtp.NewSessionSRTCP(mux.srtcp, scfg)
					if err == nil {
						// SessionSRTCP blocks its reader when a newly discovered
						// stream is not accepted. Because RTP and RTCP share the
						// packet mux, leaving RTCP unread eventually fills the RTCP
						// endpoint buffer and stalls inbound RTP as well.
						go drainSRTCP(sc)
					}
				}
			}
		}
	}
	t.setResult(dc, sr, sc, mux, err)
}

// drainSRTCP consumes control packets so they cannot apply backpressure to the
// shared RTP/RTCP packet mux. The connector does not currently use receiver
// reports, but Pion still requires every discovered SRTCP stream to be read.
func drainSRTCP(session *psrtp.SessionSRTCP) {
	for {
		stream, _, err := session.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			buf := make([]byte, 2048)
			for {
				if _, err := stream.Read(buf); err != nil {
					return
				}
			}
		}()
	}
}

func (t *Transport) setResult(dc *pdtls.Conn, sr *psrtp.SessionSRTP, sc *psrtp.SessionSRTCP, mux *packetMux, err error) {
	t.mu.Lock()
	closed := t.closed
	if !closed {
		t.dtls, t.srtp, t.srtcp, t.err = dc, sr, sc, err
	}
	t.mu.Unlock()
	if closed {
		if sr != nil {
			_ = sr.Close()
		}
		if sc != nil {
			_ = sc.Close()
		}
		if dc != nil {
			_ = dc.Close()
		}
		if mux != nil {
			_ = mux.Close()
		}
	}
}
func (t *Transport) connectICE(ctx context.Context) (net.Conn, error) {
	mux := pice.NewUDPMuxDefault(pice.UDPMuxParams{UDPConn: t.raw})
	a, err := pice.NewAgent(&pice.AgentConfig{LocalUfrag: t.conf.LocalUfrag, LocalPwd: t.conf.LocalPwd, NetworkTypes: []pice.NetworkType{pice.NetworkTypeUDP4}, CandidateTypes: []pice.CandidateType{pice.CandidateTypeHost}, NAT1To1IPs: []string{t.conf.Local.Addr().String()}, NAT1To1IPCandidateType: pice.CandidateTypeHost, UDPMux: mux})
	if err != nil {
		_ = mux.Close()
		return nil, err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = a.Close()
		_ = mux.Close()
		return nil, context.Canceled
	}
	t.iceAgent = a
	t.iceMux = mux
	t.mu.Unlock()
	done := make(chan struct{})
	var once sync.Once
	a.OnCandidate(func(c pice.Candidate) {
		if c == nil {
			once.Do(func() { close(done) })
		}
	})
	if err = a.GatherCandidates(); err != nil {
		return nil, err
	}
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	for _, rc := range t.conf.RemoteCandidates {
		c, e := pice.UnmarshalCandidate(rc.Raw)
		if e != nil {
			return nil, e
		}
		if e = a.AddRemoteCandidate(c); e != nil {
			return nil, e
		}
	}
	for {
		cs, e := a.GetRemoteCandidates()
		if e != nil {
			return nil, e
		}
		if len(cs) >= len(t.conf.RemoteCandidates) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return a.Dial(ctx, t.conf.RemoteUfrag, t.conf.RemotePwd)
}
func (t *Transport) Wait() error {
	t.Start()
	select {
	case <-t.ready:
	case <-t.ctx.Done():
		return t.ctx.Err()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.err
}
func (t *Transport) ReadRTP() (*rtp.Packet, error) {
	if err := t.Wait(); err != nil {
		return nil, err
	}
	t.readMu.Lock()
	stream := t.read
	if stream == nil {
		t.mu.RLock()
		s := t.srtp
		t.mu.RUnlock()
		if s == nil {
			t.readMu.Unlock()
			return nil, io.EOF
		}
		var err error
		stream, _, err = s.AcceptStream()
		if err != nil {
			t.readMu.Unlock()
			return nil, err
		}
		t.read = stream
	}
	t.readMu.Unlock()
	buf := make([]byte, 1600)
	for {
		n, e := stream.Read(buf)
		if e != nil {
			return nil, e
		}
		var p rtp.Packet
		if e = p.Unmarshal(buf[:n]); e == nil {
			return &p, nil
		}
	}
}
func (t *Transport) WriteRTP(p *rtp.Packet) error {
	if err := t.Wait(); err != nil {
		return err
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	t.mu.RLock()
	s := t.srtp
	t.mu.RUnlock()
	if s == nil {
		return io.EOF
	}
	if t.write == nil {
		w, e := s.OpenWriteStream()
		if e != nil {
			return e
		}
		t.write = w
	}
	b, e := p.Marshal()
	if e != nil {
		return e
	}
	_, e = t.write.Write(b)
	return e
}
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.cancel()
		t.mu.Lock()
		t.closed = true
		t.mu.Unlock()
		t.closeResources()
		t.startOnce.Do(func() { close(t.ready) })
		<-t.ready
		t.closeResources()
		_ = t.raw.Close()
	})
	return nil
}
func (t *Transport) closeResources() {
	t.mu.RLock()
	a, im, m, dc, sr, sc := t.iceAgent, t.iceMux, t.mux, t.dtls, t.srtp, t.srtcp
	t.mu.RUnlock()
	if a != nil {
		_ = a.Close()
	}
	if im != nil {
		_ = im.Close()
	}
	if m != nil {
		_ = m.Close()
	}
	if sr != nil {
		_ = sr.Close()
	}
	if sc != nil {
		_ = sc.Close()
	}
	if dc != nil {
		_ = dc.Close()
	}
}

type packet struct {
	b    []byte
	addr net.Addr
}
type endpoint struct {
	parent  *packetMux
	packets chan packet
	closed  chan struct{}
	once    sync.Once
}
type packetMux struct {
	conn              net.Conn
	dtls, srtp, srtcp *endpoint
	closed            chan struct{}
	once              sync.Once
}

func newEndpoint(m *packetMux) *endpoint {
	return &endpoint{parent: m, packets: make(chan packet, 128), closed: make(chan struct{})}
}
func newPacketMux(c net.Conn) *packetMux {
	m := &packetMux{conn: c, closed: make(chan struct{})}
	m.dtls = newEndpoint(m)
	m.srtp = newEndpoint(m)
	m.srtcp = newEndpoint(m)
	go m.readLoop()
	return m
}
func (m *packetMux) readLoop() {
	b := make([]byte, 2048)
	for {
		n, e := m.conn.Read(b)
		if e != nil {
			_ = m.Close()
			return
		}
		if n == 0 {
			continue
		}
		p := packet{append([]byte(nil), b[:n]...), m.conn.RemoteAddr()}
		var ep *endpoint
		if p.b[0] >= 20 && p.b[0] <= 63 {
			ep = m.dtls
		} else if p.b[0] >= 128 && p.b[0] <= 191 {
			if len(p.b) > 1 && p.b[1] >= 192 && p.b[1] <= 223 {
				ep = m.srtcp
			} else {
				ep = m.srtp
			}
		} else {
			continue
		}
		select {
		case ep.packets <- p:
		case <-ep.closed:
		case <-m.closed:
			return
		}
	}
}
func (m *packetMux) Close() error {
	m.once.Do(func() {
		close(m.closed)
		_ = m.conn.Close()
		_ = m.dtls.Close()
		_ = m.srtp.Close()
		_ = m.srtcp.Close()
	})
	return nil
}
func (e *endpoint) ReadFrom(b []byte) (int, net.Addr, error) {
	select {
	case <-e.closed:
		return 0, nil, io.EOF
	case p := <-e.packets:
		return copy(b, p.b), p.addr, nil
	}
}
func (e *endpoint) WriteTo(b []byte, _ net.Addr) (int, error) {
	select {
	case <-e.closed:
		return 0, io.EOF
	default:
		return e.parent.conn.Write(b)
	}
}
func (e *endpoint) Read(b []byte) (int, error)       { n, _, x := e.ReadFrom(b); return n, x }
func (e *endpoint) Write(b []byte) (int, error)      { return e.WriteTo(b, nil) }
func (e *endpoint) RemoteAddr() net.Addr             { return e.parent.conn.RemoteAddr() }
func (e *endpoint) LocalAddr() net.Addr              { return e.parent.conn.LocalAddr() }
func (e *endpoint) Close() error                     { e.once.Do(func() { close(e.closed) }); return nil }
func (e *endpoint) SetDeadline(time.Time) error      { return nil }
func (e *endpoint) SetReadDeadline(time.Time) error  { return nil }
func (e *endpoint) SetWriteDeadline(time.Time) error { return nil }
