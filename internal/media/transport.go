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
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pdtls "github.com/pion/dtls/v3"
	pice "github.com/pion/ice/v4"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	psrtp "github.com/pion/srtp/v3"
)

type Transport struct {
	conf                 *Negotiation
	raw                  *net.UDPConn
	timeout              time.Duration
	mediaTimeout         time.Duration
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
	rtcpReports          chan []rtcp.Packet
	writeMu              sync.Mutex
	write                *psrtp.WriteStreamSRTP

	// Inbound RTP is pumped from every accepted SRTP stream into rtpIn so a
	// change of the Meta SSRC mid-call is followed instead of silently ignored.
	rtpIn       chan *rtp.Packet
	pumpOnce    sync.Once
	pumpDone    chan struct{}
	pumpErr     error
	pumpErrOnce sync.Once
	sawRTP      atomic.Bool
	rxDropped   atomic.Uint64

	rx       rxStats
	tx       txStats
	rtcpSSRC uint32
	rtcpMu   sync.Mutex
	rtcpOut  *psrtp.WriteStreamSRTCP
}

// rtpQueueSize bounds buffered inbound packets (about 5 s of 20 ms audio).
const rtpQueueSize = 256

func NewTransport(parent context.Context, raw *net.UDPConn, conf *Negotiation, timeout, mediaTimeout time.Duration) *Transport {
	ctx, cancel := context.WithCancel(parent)
	return &Transport{conf: conf, raw: raw, timeout: timeout, mediaTimeout: mediaTimeout, ctx: ctx, cancel: cancel, ready: make(chan struct{}), rtcpReports: make(chan []rtcp.Packet, 32),
		rtpIn: make(chan *rtp.Packet, rtpQueueSize), pumpDone: make(chan struct{}), rtcpSSRC: rand.Uint32()}
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
						// Accept and read every RTCP stream so control traffic cannot
						// block the shared RTP/RTCP packet mux.
						go t.drainSRTCP(sc)
					}
				}
			}
		}
	}
	t.setResult(dc, sr, sc, mux, err)
	if err == nil && sr != nil {
		// Consume inbound RTP from the moment the session exists, not from the
		// first ReadRTP call, so nothing queues up unread in the shared mux.
		t.startPump(sr)
	}
}

// drainSRTCP consumes every discovered SRTCP stream. Consuming is mandatory:
// the SRTCP session shares one packet mux with RTP and DTLS, so an unread
// stream would eventually back up the mux and stall the whole call. Nothing in
// here may block: diagnostics are best effort and the sender report hook only
// takes a short mutex.
func (t *Transport) drainSRTCP(session *psrtp.SessionSRTCP) {
	for {
		stream, _, err := session.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			defer stream.Close()
			buf := make([]byte, 2048)
			for {
				n, err := stream.Read(buf)
				if err != nil {
					return
				}
				packets, err := rtcp.Unmarshal(buf[:n])
				if err != nil {
					continue
				}
				for _, pkt := range packets {
					if sr, ok := pkt.(*rtcp.SenderReport); ok {
						t.rx.noteSenderReport(sr.NTPTime, time.Now())
					}
				}
				select {
				case t.rtcpReports <- packets:
				default:
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

// streamAcceptor is the part of an SRTP session that hands out inbound RTP
// streams, one per SSRC.
type streamAcceptor interface {
	AcceptStream() (*psrtp.ReadStreamSRTP, uint32, error)
}

// ErrNoInboundRTP is returned when Meta never sends a single RTP packet.
var ErrNoInboundRTP = errors.New("no inbound RTP received")

// ErrNotReady is returned by TryWriteRTP while the media path is still being
// established.
var ErrNotReady = errors.New("meta media path not ready")

func (t *Transport) startPump(s streamAcceptor) {
	t.pumpOnce.Do(func() { go t.acceptLoop(s) })
}

// acceptLoop keeps accepting streams for the life of the session. A new SSRC
// (Meta restarting its sender, for example) gets its own reader feeding the
// same queue.
func (t *Transport) acceptLoop(s streamAcceptor) {
	for {
		stream, _, err := s.AcceptStream()
		if err != nil {
			t.pumpErrOnce.Do(func() { t.pumpErr = err; close(t.pumpDone) })
			return
		}
		go t.readStream(stream)
	}
}

// readStream copies valid RTP packets into the queue. It never blocks: if the
// consumer falls behind the oldest packet is dropped, because a backed-up
// reader would stall the shared packet mux that DTLS and RTCP also use, and
// stale audio is worth less than a live call.
func (t *Transport) readStream(r io.Reader) {
	buf := make([]byte, 1600)
	for {
		n, err := r.Read(buf)
		if err != nil {
			return
		}
		var p rtp.Packet
		if p.Unmarshal(append([]byte(nil), buf[:n]...)) != nil {
			continue
		}
		t.rx.update(&p, time.Now())
		t.sawRTP.Store(true)
		select {
		case t.rtpIn <- &p:
		default:
			select {
			case <-t.rtpIn:
				t.rxDropped.Add(1)
			default:
			}
			select {
			case t.rtpIn <- &p:
			default:
				t.rxDropped.Add(1)
			}
		}
	}
}

// ReadRTP returns the next inbound RTP packet. If nothing arrives within the
// media timeout it fails, whether no media ever started (ErrNoInboundRTP) or
// it stopped mid-call.
func (t *Transport) ReadRTP() (*rtp.Packet, error) {
	if err := t.Wait(); err != nil {
		return nil, err
	}
	t.mu.RLock()
	s := t.srtp
	t.mu.RUnlock()
	if s == nil {
		return nil, io.EOF
	}
	t.startPump(s)
	var expired <-chan time.Time
	if t.mediaTimeout > 0 {
		timer := time.NewTimer(t.mediaTimeout)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case p := <-t.rtpIn:
		return p, nil
	case <-expired:
		if t.sawRTP.Load() {
			return nil, fmt.Errorf("inbound RTP stopped: %w", os.ErrDeadlineExceeded)
		}
		return nil, fmt.Errorf("%w within %s", ErrNoInboundRTP, t.mediaTimeout)
	case <-t.pumpDone:
		return nil, fmt.Errorf("inbound RTP stopped: %w", t.pumpErr)
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	}
}

// RTCPReports exposes Meta-side feedback for diagnostics. It is independent
// of the LiveKit RTCP session and is intentionally not forwarded to it.
func (t *Transport) RTCPReports() <-chan []rtcp.Packet { return t.rtcpReports }
func (t *Transport) WriteRTP(p *rtp.Packet) error {
	if err := t.Wait(); err != nil {
		return err
	}
	return t.writeRTP(p)
}

// TryWriteRTP sends without waiting for the media path. Until it is up it
// returns ErrNotReady so callers drop the packet instead of queueing stale
// audio that would later arrive as one burst.
func (t *Transport) TryWriteRTP(p *rtp.Packet) error {
	select {
	case <-t.ready:
	default:
		return ErrNotReady
	}
	t.mu.RLock()
	err := t.err
	t.mu.RUnlock()
	if err != nil {
		return err
	}
	return t.writeRTP(p)
}

func (t *Transport) writeRTP(p *rtp.Packet) error {
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
	if _, e = t.write.Write(b); e != nil {
		return e
	}
	t.tx.sent(p, time.Now())
	return nil
}

// Stats is a snapshot of media quality for logging. It contains counters only.
type Stats struct {
	RxPackets   uint32
	RxLost      int64
	RxReordered uint64
	RxDropped   uint64
	RxJitterMs  float64
	TxPackets   uint32
}

func (t *Transport) Stats() Stats {
	rx := t.rx.snapshot()
	t.tx.mu.Lock()
	tx := t.tx.packets
	t.tx.mu.Unlock()
	return Stats{RxPackets: rx.Received, RxLost: rx.Lost, RxReordered: rx.Reordered, RxDropped: t.rxDropped.Load(), RxJitterMs: rx.JitterMs, TxPackets: tx}
}

// buildRTCP assembles the compound report sent to Meta: a sender report when
// audio has been sent (otherwise a receiver report), carrying the reception
// block for the Meta stream, plus the mandatory SDES CNAME.
func (t *Transport) buildRTCP(now time.Time) []rtcp.Packet {
	var blocks []rtcp.ReceptionReport
	if rr, ok := t.rx.report(now); ok {
		blocks = append(blocks, rr)
	}
	var head rtcp.Packet
	ssrc := t.rtcpSSRC
	if sr, ok := t.tx.senderReport(now); ok {
		sr.Reports = blocks
		head, ssrc = sr, sr.SSRC
	} else {
		head = &rtcp.ReceiverReport{SSRC: ssrc, Reports: blocks}
	}
	sdes := &rtcp.SourceDescription{Chunks: []rtcp.SourceDescriptionChunk{{
		Source: ssrc, Items: []rtcp.SourceDescriptionItem{{Type: rtcp.SDESCNAME, Text: "whatsapp-connector"}},
	}}}
	return []rtcp.Packet{head, sdes}
}

func (t *Transport) sendRTCP(now time.Time) error {
	raw, err := rtcp.Marshal(t.buildRTCP(now))
	if err != nil {
		return err
	}
	t.mu.RLock()
	sc := t.srtcp
	t.mu.RUnlock()
	if sc == nil {
		return io.EOF
	}
	t.rtcpMu.Lock()
	defer t.rtcpMu.Unlock()
	if t.rtcpOut == nil {
		if t.rtcpOut, err = sc.OpenWriteStream(); err != nil {
			return err
		}
	}
	_, err = t.rtcpOut.Write(raw)
	return err
}

// RunRTCP sends periodic receiver/sender reports to Meta until ctx ends.
// Without them Meta gets no loss or jitter feedback from the connector and
// cannot adapt its sender. Failures are ignored: reports are best effort.
func (t *Transport) RunRTCP(ctx context.Context, interval time.Duration) {
	if interval <= 0 || t.Wait() != nil {
		return
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.ctx.Done():
			return
		case now := <-tick.C:
			_ = t.sendRTCP(now)
		}
	}
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
