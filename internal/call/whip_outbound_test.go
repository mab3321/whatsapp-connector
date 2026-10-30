package call

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/livekit/protocol/livekit"
)

const rawOffer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=extmap-allow-mixed\r\na=sendrecv\r\n" +
	"a=ssrc:1234 cname:agent\r\na=msid:agent roomio_audio\r\n" +
	"a=candidate:1 1 tcp 1671430143 54.85.250.8 7881 typ host tcptype passive\r\n" +
	"a=candidate:2 1 udp 2130706431 172.17.0.1 7882 typ host\r\n" +
	"a=candidate:3 1 udp 2130706431 54.85.250.8 7882 typ host\r\n"

func (f *fakeWHIP) CreateServerOffer(_ context.Context, token string, sub map[string][]string) (string, string, error) {
	f.mu.Lock()
	f.subscribe, f.token = sub, token
	f.serverOffers++
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return "", "", err
	}
	if f.onCreate != nil {
		f.onCreate()
	}
	return rawOffer, "/whip/v1/PA_out", nil
}

func (f *fakeWHIP) SendAnswer(_ context.Context, _, _, answer string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentAnswer = answer
	return nil
}

func (f *fakeWHIP) answer() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sentAnswer
}

func dialParams() DialParams {
	return DialParams{
		PhoneID: "1", APIToken: "tok", APIVersion: "v21.0", To: "+923001234567",
		RoomName: "room-out", Identity: "wa-out",
		Agents: []*livekit.RoomAgentDispatch{{AgentName: "pentagonai"}},
	}
}

func newOutboundManager(t *testing.T, r *fakeRooms, w *fakeWHIP, ms *metaStub) *Manager {
	m := newWHIPManager(t, r, w, ms)
	m.conf.DirectOutbound = true
	m.conf.AgentTrackWait = TrackWaitServer
	return m
}

func TestWHIPOutboundCarriesServerOfferToMetaAndAnswerBack(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	w.onCreate = func() { rooms.set(agentInfo(), callerInfo("wa-out", livekit.ParticipantInfo_JOINING)) }
	m := newOutboundManager(t, rooms, w, ms)

	callID, room, err := m.Dial(context.Background(), dialParams())
	if err != nil || callID != "wacid.out" || room != "room-out" {
		t.Fatalf("Dial = %q %q %v", callID, room, err)
	}
	if w.serverOffers != 1 || w.subscribe["agent-1"][0] != "roomio_audio" {
		t.Fatalf("server offer not requested for the agent track: %v", w.subscribe)
	}
	if got := strings.Join(ms.list(), ","); got != "connect" {
		t.Fatalf("Meta actions = %s, want connect", got)
	}
	offer := ms.answers[0]
	for _, bad := range []string{"tcp", "172.17.0.1", "a=ssrc", "a=msid:", "extmap-allow-mixed", "0.0.0.0"} {
		if strings.Contains(offer, bad) {
			t.Fatalf("offer sent to Meta still has %q: %q", bad, offer)
		}
	}
	if !strings.Contains(offer, "c=IN IP4 54.85.250.8") || !strings.Contains(offer, "m=audio 7882 ") {
		t.Fatalf("offer was not given the public address: %q", offer)
	}

	// Meta's answer arrives by webhook; the server gets it and the call goes live
	rooms.set(agentInfo(), callerInfo("wa-out", livekit.ParticipantInfo_ACTIVE))
	if err := m.Connect(context.Background(), callID, "v=0 meta-answer", true); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if w.answer() != "v=0 meta-answer" {
		t.Fatalf("answer was not handed to the server: %q", w.answer())
	}
	if got := strings.Join(ms.list(), ","); got != "connect" {
		t.Fatalf("direct outbound must not touch Meta again, got %s", got)
	}

	if err := m.Disconnect(callID, true, ""); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "WHIP session delete", func() bool { return w.deletes() == 1 })
	waitFor(t, "call removal", func() bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.calls) == 0 })
}

func TestWHIPOutboundDoesNotCallMetaWhenServerOfferFails(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{err: errors.New("boom")}, newMetaStub(t)
	rooms.set(agentInfo())
	m := newOutboundManager(t, rooms, w, ms)
	if _, _, err := m.Dial(context.Background(), dialParams()); err == nil {
		t.Fatal("expected an error")
	}
	if len(ms.list()) != 0 {
		t.Fatalf("Meta must not be called: %v", ms.list())
	}
}

func TestWHIPOutboundWithoutAgentDispatchStaysOnRelay(t *testing.T) {
	m := newOutboundManager(t, &fakeRooms{}, &fakeWHIP{}, newMetaStub(t))
	p := dialParams()
	p.Agents = nil
	if m.useDirectOutbound(p) {
		t.Fatal("no agent to wait for: must not use the direct path")
	}
}

func TestWHIPOutboundDisabledByDefault(t *testing.T) {
	m := newWHIPManager(t, &fakeRooms{}, &fakeWHIP{}, newMetaStub(t))
	if m.useDirectOutbound(dialParams()) {
		t.Fatal("direct outbound must be opt-in")
	}
}

func TestWHIPOutboundUnansweredCallEndsAndCleansUp(t *testing.T) {
	rooms, w, ms := &fakeRooms{}, &fakeWHIP{}, newMetaStub(t)
	rooms.set(agentInfo())
	m := newOutboundManager(t, rooms, w, ms)
	p := dialParams()
	p.Timeout = 50 * time.Millisecond // ring time
	if _, _, err := m.Dial(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "WHIP session delete", func() bool { return w.deletes() == 1 })
	waitFor(t, "Meta terminate", func() bool { return strings.Contains(strings.Join(ms.list(), ","), "terminate") })
}
