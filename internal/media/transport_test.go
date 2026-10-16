package media

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	psrtp "github.com/pion/srtp/v3"
)

func TestImmediateCloseInterruptsICE(t *testing.T) {
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := NewCertificate()
	n, err := ParseOfferAndAnswer(testOffer, cert, netip.MustParseAddr("127.0.0.1"), udp.LocalAddr().(*net.UDPAddr).Port)
	if err != nil {
		t.Fatal(err)
	}
	tr := NewTransport(context.Background(), udp, n, 10*time.Second, 20*time.Second)
	tr.Start()
	start := time.Now()
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("close blocked for %s", elapsed)
	}
}

// pktConn is an in-memory packet connection: one Write is one Read, like UDP.
type pktConn struct {
	in     <-chan []byte
	out    chan<- []byte
	closed chan struct{}
	once   sync.Once
}

func pktPair() (*pktConn, *pktConn) {
	ab, ba := make(chan []byte, 4096), make(chan []byte, 4096)
	return &pktConn{in: ba, out: ab, closed: make(chan struct{})}, &pktConn{in: ab, out: ba, closed: make(chan struct{})}
}

func (c *pktConn) Read(b []byte) (int, error) {
	select {
	case p := <-c.in:
		return copy(b, p), nil
	case <-c.closed:
		return 0, io.EOF
	}
}
func (c *pktConn) Write(b []byte) (int, error) {
	select {
	case c.out <- append([]byte(nil), b...):
		return len(b), nil
	case <-c.closed:
		return 0, io.ErrClosedPipe
	}
}
func (c *pktConn) Close() error                     { c.once.Do(func() { close(c.closed) }); return nil }
func (c *pktConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *pktConn) RemoteAddr() net.Addr             { return &net.UDPAddr{} }
func (c *pktConn) SetDeadline(time.Time) error      { return nil }
func (c *pktConn) SetReadDeadline(time.Time) error  { return nil }
func (c *pktConn) SetWriteDeadline(time.Time) error { return nil }

func srtpConfigs() (*psrtp.Config, *psrtp.Config) {
	k1, s1 := make([]byte, 16), make([]byte, 14)
	k2, s2 := make([]byte, 16), make([]byte, 14)
	for i := range k1 {
		k1[i], k2[i] = byte(i+1), byte(100+i)
	}
	for i := range s1 {
		s1[i], s2[i] = byte(i+7), byte(200+i)
	}
	a := &psrtp.Config{Profile: psrtp.ProtectionProfileAes128CmHmacSha1_80, Keys: psrtp.SessionKeys{LocalMasterKey: k1, LocalMasterSalt: s1, RemoteMasterKey: k2, RemoteMasterSalt: s2}}
	b := &psrtp.Config{Profile: psrtp.ProtectionProfileAes128CmHmacSha1_80, Keys: psrtp.SessionKeys{LocalMasterKey: k2, LocalMasterSalt: s2, RemoteMasterKey: k1, RemoteMasterSalt: s1}}
	return a, b
}

// mediaPair returns a Transport whose SRTP/SRTCP sessions are wired to a peer
// session pair (the peer plays Meta), skipping ICE and DTLS.
func mediaPair(t *testing.T, mediaTimeout time.Duration) (tr *Transport, metaRTP *psrtp.SessionSRTP, metaRTCP *psrtp.SessionSRTCP) {
	t.Helper()
	cfgA, cfgB := srtpConfigs()
	rtpA, rtpB := pktPair()
	rtcpA, rtcpB := pktPair()
	ours, err := psrtp.NewSessionSRTP(rtpA, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	metaRTP, err = psrtp.NewSessionSRTP(rtpB, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	oursC, err := psrtp.NewSessionSRTCP(rtcpA, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	metaRTCP, err = psrtp.NewSessionSRTCP(rtcpB, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	tr = NewTransport(context.Background(), nil, nil, time.Second, mediaTimeout)
	tr.startOnce.Do(func() { close(tr.ready) })
	tr.srtp, tr.srtcp = ours, oursC
	go tr.drainSRTCP(oursC)
	t.Cleanup(func() {
		tr.cancel()
		_ = ours.Close()
		_ = oursC.Close()
		_ = metaRTP.Close()
		_ = metaRTCP.Close()
	})
	return tr, metaRTP, metaRTCP
}

func sendRTP(t *testing.T, w *psrtp.WriteStreamSRTP, ssrc uint32, seq uint16, ts uint32) {
	t.Helper()
	h := &rtp.Header{Version: 2, PayloadType: 111, SSRC: ssrc, SequenceNumber: seq, Timestamp: ts}
	if _, err := w.WriteRTP(h, []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
}

func TestReadRTPFollowsSSRCChange(t *testing.T) {
	tr, meta, _ := mediaPair(t, 2*time.Second)
	w, err := meta.OpenWriteStream()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		sendRTP(t, w, 1111, uint16(1+i), uint32(i)*960)
	}
	for i := 0; i < 3; i++ { // Meta restarts its sender with a new SSRC and numbering
		sendRTP(t, w, 2222, uint16(500+i), uint32(90000+i*960))
	}
	seen := map[uint32]int{}
	for i := 0; i < 6; i++ {
		p, err := tr.ReadRTP()
		if err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		seen[p.SSRC]++
	}
	if seen[1111] != 3 || seen[2222] != 3 {
		t.Fatalf("packets from both sources must be delivered, got %v", seen)
	}
}

func TestReadRTPTimesOutWhenMediaNeverStarts(t *testing.T) {
	tr, _, _ := mediaPair(t, 100*time.Millisecond)
	start := time.Now()
	_, err := tr.ReadRTP()
	if !errors.Is(err, ErrNoInboundRTP) {
		t.Fatalf("err = %v, want ErrNoInboundRTP", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %s", time.Since(start))
	}
}

func TestReadRTPReportsMediaStoppingMidCall(t *testing.T) {
	tr, meta, _ := mediaPair(t, 150*time.Millisecond)
	w, _ := meta.OpenWriteStream()
	sendRTP(t, w, 1, 1, 0)
	if _, err := tr.ReadRTP(); err != nil {
		t.Fatal(err)
	}
	_, err := tr.ReadRTP()
	if errors.Is(err, ErrNoInboundRTP) || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("err = %v, want a stopped-media timeout", err)
	}
}

func TestReadRTPStopsWhenTransportContextEnds(t *testing.T) {
	tr, _, _ := mediaPair(t, time.Hour)
	time.AfterFunc(30*time.Millisecond, tr.cancel)
	if _, err := tr.ReadRTP(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// If the consumer stops reading, the pump must drop old packets rather than
// block: a blocked reader backs up the shared packet mux and stalls DTLS and
// RTCP too, which fails the call.
func TestInboundPumpDropsOldestInsteadOfBlocking(t *testing.T) {
	tr, meta, _ := mediaPair(t, time.Hour)
	tr.startPump(tr.srtp)
	w, _ := meta.OpenWriteStream()
	const total = rtpQueueSize * 3
	done := make(chan struct{})
	go func() {
		for i := 0; i < total; i++ {
			sendRTP(t, w, 7, uint16(i), uint32(i)*960)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sender blocked: the pump is not draining the session")
	}
	deadline := time.Now().Add(5 * time.Second)
	for tr.Stats().RxPackets < total && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := tr.Stats().RxPackets; got != total {
		t.Fatalf("pump consumed %d of %d packets", got, total)
	}
	if tr.Stats().RxDropped == 0 {
		t.Fatal("expected old packets to be dropped while nobody was reading")
	}
	if len(tr.rtpIn) > rtpQueueSize {
		t.Fatalf("queue grew past its bound: %d", len(tr.rtpIn))
	}
}

func TestTryWriteRTPBeforeReadyDoesNotBlock(t *testing.T) {
	tr := NewTransport(context.Background(), nil, nil, time.Second, time.Second)
	done := make(chan error, 1)
	go func() { done <- tr.TryWriteRTP(&rtp.Packet{}) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotReady) {
			t.Fatalf("err = %v, want ErrNotReady", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TryWriteRTP blocked before the media path was ready")
	}
}

func TestWriteRTPCountsForSenderReports(t *testing.T) {
	tr, meta, _ := mediaPair(t, time.Second)
	if err := tr.TryWriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 42, SequenceNumber: 1, Timestamp: 960}, Payload: make([]byte, 80)}); err != nil {
		t.Fatal(err)
	}
	stream, _, err := meta.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1600)
	if n, err := stream.Read(buf); err != nil || n == 0 {
		t.Fatalf("Meta did not receive the packet: n=%d err=%v", n, err)
	}
	sr, ok := tr.tx.senderReport(time.Now())
	if !ok || sr.SSRC != 42 || sr.PacketCount != 1 || sr.OctetCount != 80 {
		t.Fatalf("sender stats not recorded: %+v ok=%v", sr, ok)
	}
}

func TestBuildRTCPCompoundPacket(t *testing.T) {
	tr, _, _ := mediaPair(t, time.Second)
	now := time.Now()

	// Nothing sent yet: a receiver report with Meta's reception block + SDES.
	for i, seq := range []uint16{1, 2, 4} {
		tr.rx.update(&rtp.Packet{Header: rtp.Header{SSRC: 5, SequenceNumber: seq, Timestamp: uint32(i) * 960}}, now)
	}
	pkts := tr.buildRTCP(now)
	rr, ok := pkts[0].(*rtcp.ReceiverReport)
	if !ok || len(rr.Reports) != 1 || rr.Reports[0].SSRC != 5 || rr.Reports[0].TotalLost != 1 {
		t.Fatalf("unexpected receiver report: %#v", pkts[0])
	}
	if _, ok := pkts[1].(*rtcp.SourceDescription); !ok {
		t.Fatalf("second packet must be SDES, got %T", pkts[1])
	}

	// Once audio has been sent it becomes a sender report with the same block.
	tr.tx.sent(&rtp.Packet{Header: rtp.Header{SSRC: 77, Timestamp: 1000}, Payload: make([]byte, 50)}, now)
	pkts = tr.buildRTCP(now)
	sr, ok := pkts[0].(*rtcp.SenderReport)
	if !ok || sr.SSRC != 77 || len(sr.Reports) != 1 {
		t.Fatalf("unexpected sender report: %#v", pkts[0])
	}
	raw, err := rtcp.Marshal(pkts)
	if err != nil {
		t.Fatal(err)
	}
	if back, err := rtcp.Unmarshal(raw); err != nil || len(back) != 2 {
		t.Fatalf("compound packet does not round-trip: %v %d", err, len(back))
	}
}

func TestSendRTCPReachesMeta(t *testing.T) {
	tr, _, metaRTCP := mediaPair(t, time.Second)
	if err := tr.sendRTCP(time.Now()); err != nil {
		t.Fatal(err)
	}
	stream, _, err := metaRTCP.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	pkts, err := rtcp.Unmarshal(buf[:n])
	if err != nil || len(pkts) != 2 {
		t.Fatalf("Meta received malformed RTCP: %v (%d packets)", err, len(pkts))
	}
	if _, ok := pkts[0].(*rtcp.ReceiverReport); !ok {
		t.Fatalf("first packet = %T, want receiver report", pkts[0])
	}
}

// Inbound RTCP must always be consumed, or the shared mux backs up and the
// call fails. Flood the connector with sender reports from many sources and
// check that the sender is never blocked and the reports are recorded.
func TestInboundRTCPIsAlwaysConsumed(t *testing.T) {
	tr, _, metaRTCP := mediaPair(t, time.Second)
	w, err := metaRTCP.OpenWriteStream()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 400; i++ { // far more than the report channel can hold
			sr := &rtcp.SenderReport{SSRC: uint32(1000 + i%7), NTPTime: ntpTime(time.Now()), PacketCount: uint32(i)}
			raw, _ := sr.Marshal()
			if _, err := w.Write(raw); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RTCP sender blocked: inbound SRTCP is not being consumed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tr.rx.mu.Lock()
		got := tr.rx.lastSR
		tr.rx.mu.Unlock()
		if got != 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("sender reports were consumed but not recorded for RTT estimation")
}
