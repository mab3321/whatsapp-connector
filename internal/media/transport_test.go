package media

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"
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
	tr := NewTransport(context.Background(), udp, n, 10*time.Second)
	tr.Start()
	start := time.Now()
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("close blocked for %s", elapsed)
	}
}
