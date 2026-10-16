package media

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

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

type blockingAcceptor struct{ release chan struct{} }

func (b blockingAcceptor) AcceptStream() (*psrtp.ReadStreamSRTP, uint32, error) {
	<-b.release
	return nil, 0, io.EOF
}

// A call whose media never starts must end instead of waiting forever in
// AcceptStream, which is what kept a dead WhatsApp call connected for 33 hours.
func TestAcceptStreamTimesOutWhenNoRTPEverArrives(t *testing.T) {
	a := blockingAcceptor{release: make(chan struct{})}
	defer close(a.release)
	start := time.Now()
	_, err := acceptStream(context.Background(), a, 50*time.Millisecond)
	if !errors.Is(err, ErrNoInboundRTP) {
		t.Fatalf("err = %v, want ErrNoInboundRTP", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %s", elapsed)
	}
}

func TestAcceptStreamStopsWhenTransportIsClosed(t *testing.T) {
	a := blockingAcceptor{release: make(chan struct{})}
	defer close(a.release)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	if _, err := acceptStream(ctx, a, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

type readyAcceptor struct{}

func (readyAcceptor) AcceptStream() (*psrtp.ReadStreamSRTP, uint32, error) { return nil, 7, nil }

func TestAcceptStreamReturnsWhenRTPArrives(t *testing.T) {
	if _, err := acceptStream(context.Background(), readyAcceptor{}, time.Second); err != nil {
		t.Fatalf("err = %v", err)
	}
}
