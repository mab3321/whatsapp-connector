package whip

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestCreateServerOfferSendsEmptyBodyWithHeader(t *testing.T) {
	var gotHeader, gotBody, gotInfo string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-LiveKit-Server-Offer")
		gotInfo = r.Header.Get("X-LiveKit-ClientInfo")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Location", "/whip/v1/PA_1")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("v=0 offer"))
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	offer, loc, err := c.CreateServerOffer(context.Background(), "tok", map[string][]string{"agent": {"roomio_audio"}})
	if err != nil || offer != "v=0 offer" || loc != "/whip/v1/PA_1" {
		t.Fatalf("got %q %q %v", offer, loc, err)
	}
	if gotHeader != "1" || gotBody != "" || !strings.Contains(gotInfo, "roomio_audio") {
		t.Fatalf("header=%q body=%q info=%q", gotHeader, gotBody, gotInfo)
	}
}

func TestSendAnswerPatchesSDP(t *testing.T) {
	var method, ctype, body, auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, ctype, auth = r.Method, r.Header.Get("Content-Type"), r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	if err := c.SendAnswer(context.Background(), "tok", "/whip/v1/PA_1", "v=0 answer"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPatch || ctype != "application/sdp" || body != "v=0 answer" || auth != "Bearer tok" {
		t.Fatalf("%s %s %q %q", method, ctype, body, auth)
	}
	if err := c.SendAnswer(context.Background(), "tok", "/elsewhere", "x"); err == nil {
		t.Fatal("a foreign location must be refused")
	}
}

func TestSendAnswerReportsServerRefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad answer"}`))
	}))
	defer srv.Close()
	err := (&Client{BaseURL: srv.URL}).SendAnswer(context.Background(), "t", "/whip/v1/PA_1", "x")
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("got %v", err)
	}
}

func TestNormalizeOfferKeepsOnlyWhatMetaAccepts(t *testing.T) {
	in := "v=0\r\no=- 1 2 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\na=extmap-allow-mixed\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\n" +
		"a=rtcp-rsize\r\na=rtpmap:111 opus/48000/2\r\na=ssrc:7 cname:x\r\na=msid:s t\r\n" +
		"a=candidate:1 1 udp 2130706431 10.0.0.5 7882 typ host\r\na=candidate:2 1 udp 2130706431 54.85.250.8 7882 typ host\r\na=setup:actpass\r\n"
	got := NormalizeOffer(in, netip.MustParseAddr("54.85.250.8"))
	for _, bad := range []string{"extmap-allow-mixed", "rtcp-rsize", "a=ssrc", "a=msid:", "10.0.0.5"} {
		if strings.Contains(got, bad) {
			t.Fatalf("still contains %q:\n%s", bad, got)
		}
	}
	for _, want := range []string{"m=audio 7882 UDP/TLS/RTP/SAVPF 111\r\n", "c=IN IP4 54.85.250.8\r\n", "a=rtpmap:111 opus/48000/2\r\n", "a=setup:actpass\r\n"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q:\n%s", want, got)
		}
	}
	if !strings.HasSuffix(got, "\r\n") || strings.Contains(got, "\r\n\r\n") {
		t.Fatalf("bad line endings: %q", got)
	}
}
