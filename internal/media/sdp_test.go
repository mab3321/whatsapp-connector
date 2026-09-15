package media

import (
	"net/netip"
	"strings"
	"testing"
)

const testOffer = "v=0\r\n" +
	"o=- 1 1 IN IP4 198.51.100.10\r\ns=-\r\nt=0 0\r\n" +
	"a=fingerprint:sha-256 AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA\r\n" +
	"m=audio 3480 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 198.51.100.10\r\n" +
	"a=mid:0\r\na=setup:actpass\r\na=rtcp-mux\r\n" +
	"a=ice-ufrag:remote-user\r\na=ice-pwd:remote-password-value\r\n" +
	"a=candidate:dead 1 udp 2130706431 192.0.2.1 9 typ host generation 0\r\n" +
	"a=candidate:live 1 udp 1694498815 198.51.100.10 3480 typ srflx raddr 10.0.0.1 rport 5000 generation 0\r\n" +
	"a=candidate:rtcp 2 udp 2130706430 198.51.100.10 3481 typ host\r\n" +
	"a=candidate:v6 1 udp 2130706431 2001:db8::1 3480 typ host\r\n" +
	"a=rtpmap:111 opus/48000/2\r\n"

func TestParseOfferAndAnswer(t *testing.T) {
	cert, err := NewCertificate()
	if err != nil {
		t.Fatal(err)
	}
	n, err := ParseOfferAndAnswer(testOffer, cert, netip.MustParseAddr("203.0.113.20"), 40001)
	if err != nil {
		t.Fatal(err)
	}
	if len(n.RemoteCandidates) != 2 {
		t.Fatalf("got %d candidates", len(n.RemoteCandidates))
	}
	if n.PayloadType != 111 || n.LocalSetup != "passive" {
		t.Fatalf("bad negotiation: %#v", n)
	}
	for _, want := range []string{"m=audio 40001 UDP/TLS/RTP/SAVPF 111", "a=rtcp-mux", "a=ice-ufrag:", "a=fingerprint:sha-256 " + cert.Fingerprint} {
		if !strings.Contains(n.Answer, want) {
			t.Errorf("answer missing %q", want)
		}
	}
}

func TestRejectsNonOpusAndMissingFingerprint(t *testing.T) {
	cert, _ := NewCertificate()
	for name, offer := range map[string]string{
		"codec":       strings.Replace(testOffer, "opus/48000/2", "PCMU/8000", 1),
		"fingerprint": strings.Replace(testOffer, "a=fingerprint:", "a=x-fingerprint:", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseOfferAndAnswer(offer, cert, netip.MustParseAddr("203.0.113.20"), 40001); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestOutboundOfferAndAnswer(t *testing.T) {
	cert, err := NewCertificate()
	if err != nil {
		t.Fatal(err)
	}
	n, err := NewOffer(cert, netip.MustParseAddr("203.0.113.20"), 40002)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"m=audio 40002 UDP/TLS/RTP/SAVPF 111", "a=setup:actpass", "a=candidate:1 1 udp", "a=ice-ufrag:" + n.LocalUfrag} {
		if !strings.Contains(n.Answer, want) {
			t.Errorf("offer missing %q", want)
		}
	}
	answer := strings.Replace(testOffer, "a=setup:actpass", "a=setup:active", 1)
	if err = ApplyAnswer(answer, n); err != nil {
		t.Fatal(err)
	}
	if n.LocalSetup != "passive" || n.IsClient || len(n.RemoteCandidates) != 2 || n.PayloadType != 111 {
		t.Fatalf("bad outbound negotiation: %#v", n)
	}
}
