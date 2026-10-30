package call

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/mab3321/whatsapp-connector/internal/meta"
)

type fakeRooms struct {
	mu          sync.Mutex
	parts       []*livekit.ParticipantInfo
	dispatches  []string
	ensureErr   error
	listErr     error
	removed     []string
	ensureCalls int
}

func (f *fakeRooms) EnsureRoom(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	return f.ensureErr
}
func (f *fakeRooms) CreateDispatch(_ context.Context, _, name, md string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatches = append(f.dispatches, name+"|"+md)
	return nil
}
func (f *fakeRooms) ListParticipants(context.Context, string) ([]*livekit.ParticipantInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]*livekit.ParticipantInfo(nil), f.parts...), nil
}
func (f *fakeRooms) RemoveParticipant(_ context.Context, _, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, id)
	return nil
}
func (f *fakeRooms) set(parts ...*livekit.ParticipantInfo) {
	f.mu.Lock()
	f.parts = parts
	f.mu.Unlock()
}

type fakeWHIP struct {
	mu        sync.Mutex
	subscribe map[string][]string
	offer     string
	err       error
	deleted   int
	onCreate  func()
	token     string
	budget    time.Duration // time left on the request context when Create ran
}

const rawAnswer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n" +
	"a=candidate:1 1 tcp 1671430143 54.85.250.8 7881 typ host tcptype passive\r\n" +
	"a=candidate:2 1 udp 2130706431 172.17.0.1 7882 typ host\r\n" +
	"a=candidate:3 1 udp 2130706431 54.85.250.8 7882 typ host\r\n"

func (f *fakeWHIP) Create(ctx context.Context, token, offer string, sub map[string][]string) (string, string, error) {
	f.mu.Lock()
	f.subscribe, f.offer, f.token = sub, offer, token
	if d, ok := ctx.Deadline(); ok {
		f.budget = time.Until(d)
	}
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return "", "", err
	}
	if token == "" {
		return "", "", errors.New("no token")
	}
	if f.onCreate != nil {
		f.onCreate()
	}
	return rawAnswer, "/whip/v1/PA_test", nil
}
func (f *fakeWHIP) Delete(context.Context, string, string) error {
	f.mu.Lock()
	f.deleted++
	f.mu.Unlock()
	return nil
}
func (f *fakeWHIP) deletes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleted
}

type metaStub struct {
	mu      sync.Mutex
	actions []string
	answers []string
	srv     *httptest.Server
}

func newMetaStub(t *testing.T) *metaStub {
	s := &metaStub{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Action  string `json:"action"`
			Session struct {
				SDP string `json:"sdp"`
			} `json:"session"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		s.mu.Lock()
		s.actions = append(s.actions, body.Action)
		s.answers = append(s.answers, body.Session.SDP)
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(s.srv.Close)
	return s
}
func (s *metaStub) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.actions...)
}

func agentInfo() *livekit.ParticipantInfo {
	return &livekit.ParticipantInfo{Identity: "agent-1", Kind: livekit.ParticipantInfo_AGENT, State: livekit.ParticipantInfo_ACTIVE,
		Tracks: []*livekit.TrackInfo{{Type: livekit.TrackType_AUDIO, Name: "roomio_audio"}}}
}
func callerInfo(id string, st livekit.ParticipantInfo_State) *livekit.ParticipantInfo {
	return &livekit.ParticipantInfo{Identity: id, Kind: livekit.ParticipantInfo_CONNECTOR, State: st}
}

func newWHIPManager(t *testing.T, r *fakeRooms, w *fakeWHIP, ms *metaStub) *Manager {
	t.Helper()
	mc := meta.New()
	mc.BaseURL = ms.srv.URL
	mc.Backoff = func(int) time.Duration { return time.Millisecond }
	m := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		APIKey: "key", APISecret: "secretsecretsecretsecretsecretsecret", PublicIP: netip.MustParseAddr("54.85.250.8"),
		PortMin: 40000, PortMax: 40100, SetupTimeout: time.Second,
		InboundMode: ModeWHIP, AgentWaitTimeout: 400 * time.Millisecond, PollInterval: 10 * time.Millisecond,
		MaxCallDuration: time.Minute,
	}, mc)
	m.rooms, m.whip = r, w
	return m
}

func params() AcceptParams {
	return AcceptParams{
		PhoneID: "1", APIToken: "tok", APIVersion: "v21.0", CallID: "wacid.1", SDP: "v=0 offer",
		RoomName: "room-1", Identity: "wa-1", Wait: true, Timeout: time.Second,
		Agents: []*livekit.RoomAgentDispatch{{AgentName: "pentagonai", Metadata: `{"a":1}`}},
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestWHIPInboundHappyPathThenUserHangup(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) }
	m := newWHIPManager(t, rooms, w, ms)

	room, err := m.Accept(context.Background(), params())
	if err != nil || room != "room-1" {
		t.Fatalf("Accept = %q, %v", room, err)
	}
	if got := strings.Join(ms.list(), ","); got != "pre_accept,accept" {
		t.Fatalf("Meta actions = %s, want pre_accept,accept", got)
	}
	if w.subscribe["agent-1"][0] != "roomio_audio" || len(w.subscribe) != 1 {
		t.Fatalf("subscribe = %v", w.subscribe)
	}
	if rooms.dispatches[0] != `pentagonai|{"a":1}` {
		t.Fatalf("dispatches = %v", rooms.dispatches)
	}
	for _, a := range ms.answers {
		if strings.Contains(a, "tcp") || strings.Contains(a, "172.17.0.1") || !strings.Contains(a, "54.85.250.8 7882") {
			t.Fatalf("answer sent to Meta was not filtered: %q", a)
		}
	}

	if err := m.Disconnect("wacid.1", true, ""); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "WHIP session delete", func() bool { return w.deletes() == 1 })
	waitFor(t, "call removal", func() bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.calls) == 0 })
	if got := strings.Join(ms.list(), ","); got != "pre_accept,accept" {
		t.Fatalf("user hangup must not terminate at Meta, got %s", got)
	}
}

func TestWHIPInboundFailsWithoutAgentAndNeverTouchesMeta(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	m := newWHIPManager(t, rooms, w, ms)
	if _, err := m.Accept(context.Background(), params()); err == nil {
		t.Fatal("expected an error when no agent joins")
	}
	if len(ms.list()) != 0 || w.subscribe != nil {
		t.Fatalf("nothing should be signalled: meta=%v whip=%v", ms.list(), w.subscribe)
	}
}

func TestWHIPInboundAgentWithoutAudioTrackFailsClearly(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	silent := agentInfo()
	silent.Tracks = nil // the current worker: joined, but publishes only after the caller
	rooms.set(silent)
	m := newWHIPManager(t, rooms, w, ms)
	_, err := m.Accept(context.Background(), params())
	if err == nil || !strings.Contains(err.Error(), "audio track") {
		t.Fatalf("want an audio-track error, got %v", err)
	}
	if w.subscribe != nil || len(ms.list()) != 0 {
		t.Fatalf("must not post an offer or signal Meta: whip=%v meta=%v", w.subscribe, ms.list())
	}
}

func TestWHIPInboundServerWaitPostsOnceAgentJoined(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	silent := agentInfo()
	silent.Tracks = nil // joined, audio not published yet: a patched server holds the offer for it
	rooms.set(silent)
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) }
	m := newWHIPManager(t, rooms, w, ms)
	m.conf.AgentTrackWait = TrackWaitServer
	if _, err := m.Accept(context.Background(), params()); err != nil {
		t.Fatal(err)
	}
	if w.subscribe["agent-1"][0] != "roomio_audio" {
		t.Fatalf("subscribe = %v", w.subscribe)
	}
	if got := strings.Join(ms.list(), ","); got != "pre_accept,accept" {
		t.Fatalf("Meta actions = %s", got)
	}
}

func TestWHIPInboundCreateFailure(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{err: errors.New("boom")}, newMetaStub(t)
	rooms.set(agentInfo())
	m := newWHIPManager(t, rooms, w, ms)
	if _, err := m.Accept(context.Background(), params()); err == nil {
		t.Fatal("expected WHIP error")
	}
	if len(ms.list()) != 0 {
		t.Fatalf("Meta must not be signalled, got %v", ms.list())
	}
}

func TestWHIPInboundMediaNeverConnects(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_JOINED)) }
	m := newWHIPManager(t, rooms, w, ms)
	m.conf.SetupTimeout = 80 * time.Millisecond
	p := params()
	p.Timeout = 80 * time.Millisecond
	if _, err := m.Accept(context.Background(), p); err == nil {
		t.Fatal("expected media timeout")
	}
	// pre_accept was sent, so the call is terminated at Meta; accept never is.
	waitFor(t, "terminate", func() bool { return strings.Join(ms.list(), ",") == "pre_accept,terminate" })
	waitFor(t, "WHIP delete", func() bool { return w.deletes() == 1 })
}

func TestWHIPInboundAgentLeavesTerminatesCall(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) }
	m := newWHIPManager(t, rooms, w, ms)
	if _, err := m.Accept(context.Background(), params()); err != nil {
		t.Fatal(err)
	}
	rooms.set(callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) // agent gone
	waitFor(t, "terminate", func() bool { return strings.HasSuffix(strings.Join(ms.list(), ","), "accept,terminate") })
	waitFor(t, "WHIP delete", func() bool { return w.deletes() == 1 })
}

func TestWHIPInboundCallerDropTerminatesCall(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) }
	m := newWHIPManager(t, rooms, w, ms)
	if _, err := m.Accept(context.Background(), params()); err != nil {
		t.Fatal(err)
	}
	rooms.set(agentInfo()) // media dropped, server removed the caller
	waitFor(t, "terminate", func() bool { return strings.HasSuffix(strings.Join(ms.list(), ","), "accept,terminate") })
}

func TestWHIPInboundDuplicateAcceptIsIdempotent(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) }
	m := newWHIPManager(t, rooms, w, ms)
	if _, err := m.Accept(context.Background(), params()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Accept(context.Background(), params()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(ms.list(), ","); got != "pre_accept,accept" {
		t.Fatalf("duplicate accept re-signalled Meta: %s", got)
	}
}

func TestUseWHIPRequiresModeAndAgents(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	m := newWHIPManager(t, rooms, w, ms)
	p := params()
	if !m.useWHIP(p) {
		t.Fatal("whip mode with agents must use WHIP")
	}
	p.Agents = nil
	if m.useWHIP(p) {
		t.Fatal("no agents: must fall back to relay")
	}
	m.conf.WhipAutoDispatch = true
	if !m.useWHIP(p) {
		t.Fatal("auto dispatch opt-in must use WHIP without explicit agents")
	}
	m.conf.WhipAutoDispatch = false
	m.conf.InboundMode = ModeRelay
	p.Agents = params().Agents
	if m.useWHIP(p) {
		t.Fatal("relay mode must not use WHIP")
	}
}

func TestHTTPURL(t *testing.T) {
	for in, want := range map[string]string{"ws://127.0.0.1:7880": "http://127.0.0.1:7880", "wss://x.example": "https://x.example", "http://h": "http://h"} {
		if got := httpURL(in); got != want {
			t.Errorf("httpURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func tokenAttributes(t *testing.T, token string) map[string]string {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWT: %q", token)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims struct {
		Attributes map[string]string `json:"attributes"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	return claims.Attributes
}

func TestWHIPInboundTellsTheWorkerThatMediaIsDirect(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) }
	m := newWHIPManager(t, rooms, w, ms)
	p := params()
	p.Attributes = map[string]string{"call.human_number": "923001234567"}

	if _, err := m.Accept(context.Background(), p); err != nil {
		t.Fatal(err)
	}

	attrs := tokenAttributes(t, w.token)
	if attrs[MediaModeAttribute] != MediaModeDirect || attrs["call.human_number"] != "923001234567" {
		t.Fatalf("token attributes = %v", attrs)
	}
	if _, leaked := p.Attributes[MediaModeAttribute]; leaked {
		t.Fatal("the caller's attribute map must not be modified")
	}
}

func TestWHIPRequestHasATwelveSecondDeadline(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-1", livekit.ParticipantInfo_ACTIVE)) }
	m := newWHIPManager(t, rooms, w, ms)

	if _, err := m.Accept(context.Background(), params()); err != nil {
		t.Fatal(err)
	}

	if w.budget <= 11*time.Second || w.budget > 12*time.Second {
		t.Fatalf("WHIP request budget = %s, want just under 12s", w.budget)
	}
}

func TestDirectMediaCanaryUsesAShareOfCalls(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	m := newWHIPManager(t, rooms, w, ms)
	m.conf.DirectPercent = 50
	direct := 0
	for i := 0; i < 400; i++ {
		if m.useWHIP(params()) {
			direct++
		}
	}
	if direct < 100 || direct > 300 {
		t.Fatalf("%d of 400 calls went direct at 50%%", direct)
	}
	m.conf.DirectPercent = 100
	for i := 0; i < 50; i++ {
		if !m.useWHIP(params()) {
			t.Fatal("100% must always use direct media")
		}
	}
}
