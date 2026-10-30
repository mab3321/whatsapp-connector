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

func TestCreateSendsOfferTokenAndSubscriptions(t *testing.T) {
	var gotCT, gotAuth, gotInfo, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotCT, gotAuth, gotInfo, gotBody = r.Header.Get("Content-Type"), r.Header.Get("Authorization"), r.Header.Get("X-LiveKit-ClientInfo"), string(b)
		w.Header().Set("Location", "/whip/v1/PA_1")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("answer-sdp"))
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	ans, loc, err := c.Create(context.Background(), "jwt", "offer-sdp", map[string][]string{"agent-1": {"roomio_audio"}})
	if err != nil || ans != "answer-sdp" || loc != "/whip/v1/PA_1" {
		t.Fatalf("Create = %q %q %v", ans, loc, err)
	}
	if gotCT != "application/sdp" || gotAuth != "Bearer jwt" || gotBody != "offer-sdp" {
		t.Fatalf("bad request: %q %q %q", gotCT, gotAuth, gotBody)
	}
	if !strings.Contains(gotInfo, `"agent-1":["roomio_audio"]`) {
		t.Fatalf("client info = %s", gotInfo)
	}
}

func TestCreateRejectsNon201AndMissingLocation(t *testing.T) {
	status, loc := http.StatusBadRequest, ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if loc != "" {
			w.Header().Set("Location", loc)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("nope\nline"))
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	if _, _, err := c.Create(context.Background(), "t", "o", nil); err == nil || strings.Contains(err.Error(), "\n") {
		t.Fatalf("want one-line HTTP error, got %v", err)
	}
	status = http.StatusCreated
	if _, _, err := c.Create(context.Background(), "t", "o", nil); err == nil {
		t.Fatal("a 201 without a Location must fail")
	}
}

func TestDelete(t *testing.T) {
	var method, path string
	code := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		w.WriteHeader(code)
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	if err := c.Delete(context.Background(), "t", "/whip/v1/PA_1"); err != nil || method != http.MethodDelete || path != "/whip/v1/PA_1" {
		t.Fatalf("Delete: %v %s %s", err, method, path)
	}
	code = http.StatusNotFound
	if err := c.Delete(context.Background(), "t", "/whip/v1/PA_1"); err != nil {
		t.Fatalf("404 means already gone, got %v", err)
	}
	code = http.StatusInternalServerError
	if err := c.Delete(context.Background(), "t", "/whip/v1/PA_1"); err == nil {
		t.Fatal("500 must error")
	}
	if err := c.Delete(context.Background(), "t", "/elsewhere"); err == nil {
		t.Fatal("a foreign location must be refused")
	}
}

func TestFilterAnswer(t *testing.T) {
	ip := netip.MustParseAddr("54.85.250.8")
	in := "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n" +
		"a=candidate:1 1 tcp 1 54.85.250.8 7881 typ host tcptype passive\r\n" +
		"a=candidate:2 1 udp 2 172.17.0.1 7882 typ host\r\n" +
		"a=candidate:3 1 udp 2 54.85.250.8 7882 typ host\r\n" +
		"a=sendrecv\r\n"
	out := FilterAnswer(in, ip)
	if strings.Contains(out, "tcp") || strings.Contains(out, "172.17") || !strings.Contains(out, "candidate:3") || !strings.Contains(out, "a=sendrecv") {
		t.Fatalf("unexpected filter result:\n%s", out)
	}
	// Never strip every candidate.
	only := "a=candidate:2 1 udp 2 172.17.0.1 7882 typ host\r\n"
	if got := FilterAnswer(only, ip); got != only {
		t.Fatalf("answer without public candidates must be unchanged, got %q", got)
	}
}
