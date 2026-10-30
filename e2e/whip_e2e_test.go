//go:build e2e

// End-to-end check of INBOUND_MODE=whip against a real LiveKit server.
//
// It plays Meta (a pion peer plus a stub of the Graph calls API) and the voice
// worker (an agent that joins the room first and publishes its audio only after
// the caller shows up, exactly like the real worker). Run it next to a connector
// started with INBOUND_MODE=whip and META_API_BASE_URL pointing at the stub:
//
//	E2E_CONNECTOR_URL=http://127.0.0.1:8091 E2E_STUB_ADDR=127.0.0.1:18080 \
//	LIVEKIT_API_KEY=... LIVEKIT_API_SECRET=... go test -tags e2e -count=1 -v ./e2e
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

var silence = []byte{0xf8, 0xff, 0xfe} // Opus silence frame

func env(t *testing.T, k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		v = def
	}
	if v == "" {
		t.Skipf("%s not set", k)
	}
	return v
}

type stub struct {
	mu      sync.Mutex
	actions []string
	answer  chan string
}

func startStub(t *testing.T, addr string) *stub {
	s := &stub{answer: make(chan string, 4)}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Action  string `json:"action"`
			Session struct {
				SDP string `json:"sdp"`
			} `json:"session"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &b)
		s.mu.Lock()
		s.actions = append(s.actions, b.Action)
		s.mu.Unlock()
		if b.Action == "pre_accept" {
			s.answer <- b.Session.SDP
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	srv.Listener.Close()
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)
	return s
}

func (s *stub) list() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.actions, ",")
}

func pump(w interface{ WriteRTP(*rtp.Packet) error }, stop <-chan struct{}) {
	var seq uint16
	var ts uint32
	tk := time.NewTicker(20 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tk.C:
			seq++
			ts += 960
			_ = w.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: seq, Timestamp: ts, SSRC: 4321}, Payload: silence})
		}
	}
}

type result struct {
	callerRx, agentRx *atomic.Int64
	agentLeave        chan struct{}
}

// runCall places one inbound call through the connector and returns once the
// connector reports it answered.
func runCall(t *testing.T, name string, st *stub, agentLeavesAfter time.Duration) (res result, room string, finish func()) {
	key, secret := env(t, "LIVEKIT_API_KEY", ""), env(t, "LIVEKIT_API_SECRET", "")
	connURL := env(t, "E2E_CONNECTOR_URL", "http://127.0.0.1:8091")
	lkHTTP := env(t, "E2E_LIVEKIT_HTTP", "http://127.0.0.1:7880")
	room = fmt.Sprintf("e2e-%s-%d", name, time.Now().Unix())
	callID := "wacid.e2e." + name
	stop := make(chan struct{})
	res = result{callerRx: &atomic.Int64{}, agentRx: &atomic.Int64{}, agentLeave: make(chan struct{})}
	rsc := lksdk.NewRoomServiceClient(lkHTTP, key, secret)

	// Worker: join as soon as the room exists, publish only after the caller joined.
	go func() {
		ctx := context.Background()
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			if rs, err := rsc.ListRooms(ctx, &livekit.ListRoomsRequest{Names: []string{room}}); err != nil || len(rs.Rooms) == 0 {
				continue
			}
			break
		}
		tok, _ := auth.NewAccessToken(key, secret).SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: room}).
			SetIdentity("agent-e2e").SetKind(livekit.ParticipantInfo_AGENT).SetValidFor(10 * time.Minute).ToJWT()
		published := make(chan struct{})
		var once sync.Once
		r, err := lksdk.ConnectToRoomWithToken(strings.Replace(lkHTTP, "http", "ws", 1), tok, &lksdk.RoomCallback{
			ParticipantCallback: lksdk.ParticipantCallback{
				OnTrackSubscribed: func(tr *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, _ *lksdk.RemoteParticipant) {
					go func() {
						for {
							if _, _, e := tr.ReadRTP(); e != nil {
								return
							}
							res.agentRx.Add(1)
						}
					}()
				},
			},
			OnParticipantConnected: func(rp *lksdk.RemoteParticipant) {
				if rp.Identity() != "wa-e2e" {
					return
				}
				once.Do(func() { close(published) })
			},
		}, lksdk.WithAutoSubscribe(true))
		if err != nil {
			t.Errorf("agent join: %v", err)
			return
		}
		defer r.Disconnect()
		for _, rp := range r.GetRemoteParticipants() {
			if rp.Identity() == "wa-e2e" {
				once.Do(func() { close(published) })
			}
		}
		// LiveKit's WHIP endpoint can only send audio from tracks that exist when the
		// offer arrives, so the worker must publish before the caller joins. Set
		// E2E_WORKER_PUBLISHES=after_caller to reproduce the current Python worker
		// (publishes only after wait_for_participant), which this mode cannot serve.
		if os.Getenv("E2E_WORKER_PUBLISHES") == "after_caller" {
			select {
			case <-published:
			case <-stop:
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		tr, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "a", "s")
		if _, err = r.LocalParticipant.PublishTrack(tr, &lksdk.TrackPublicationOptions{Name: "roomio_audio", Source: livekit.TrackSource_MICROPHONE}); err != nil {
			t.Errorf("agent publish: %v", err)
			return
		}
		go pump(tr, stop)
		if agentLeavesAfter > 0 {
			select {
			case <-time.After(agentLeavesAfter):
				close(res.agentLeave)
			case <-stop:
			}
			return
		}
		<-stop
	}()

	// Meta-like caller: Opus + telephone-event, sendrecv.
	me := &webrtc.MediaEngine{}
	_ = me.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"}, PayloadType: 111}, webrtc.RTPCodecTypeAudio)
	_ = me.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "audio/telephone-event", ClockRate: 8000}, PayloadType: 126}, webrtc.RTPCodecTypeAudio)
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(me)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	ct, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "c", "s")
	if _, err = pc.AddTransceiverFromTrack(ct, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv}); err != nil {
		t.Fatal(err)
	}
	pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			for {
				if _, _, e := tr.ReadRTP(); e != nil {
					return
				}
				res.callerRx.Add(1)
			}
		}()
	})
	offer, _ := pc.CreateOffer(nil)
	gathered := webrtc.GatheringCompletePromise(pc)
	_ = pc.SetLocalDescription(offer)
	<-gathered

	// Apply Meta's pre_accept answer to the peer as soon as the connector sends it.
	go func() {
		select {
		case ans := <-st.answer:
			if e := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: ans}); e != nil {
				t.Errorf("set answer: %v", e)
				return
			}
			go pump(ct, stop)
		case <-stop:
		}
	}()

	atok, _ := auth.NewAccessToken(key, secret).SetVideoGrant(&auth.VideoGrant{RoomCreate: true}).SetValidFor(10 * time.Minute).ToJWT()
	hc := &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		r.Header.Set("Authorization", "Bearer "+atok)
		return http.DefaultTransport.RoundTrip(r)
	})}
	cli := livekit.NewConnectorProtobufClient(connURL, hc)
	started := time.Now()
	_, err = cli.AcceptWhatsAppCall(context.Background(), &livekit.AcceptWhatsAppCallRequest{
		WhatsappPhoneNumberId: "1", WhatsappApiKey: "tok", WhatsappCloudApiVersion: "v21.0", WhatsappCallId: callID,
		Sdp: &livekit.SessionDescription{Type: "offer", Sdp: pc.LocalDescription().SDP}, RoomName: room,
		ParticipantIdentity: "wa-e2e", ParticipantName: "WA caller", ParticipantAttributes: map[string]string{"k": "v"},
		Agents:            []*livekit.RoomAgentDispatch{{AgentName: "e2e-agent", Metadata: `{"e2e":true}`}},
		WaitUntilAnswered: true,
	})
	if err != nil {
		close(stop)
		_ = pc.Close()
		t.Fatalf("AcceptWhatsAppCall: %v", err)
	}
	t.Logf("%s: answered after %v", name, time.Since(started).Round(time.Millisecond))

	var once sync.Once
	finish = func() {
		once.Do(func() {
			_, _ = cli.DisconnectWhatsAppCall(context.Background(), &livekit.DisconnectWhatsAppCallRequest{
				WhatsappCallId: callID, DisconnectReason: livekit.DisconnectWhatsAppCallRequest_USER_INITIATED,
			})
			close(stop)
			_ = pc.Close()
		})
	}
	return res, room, finish
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func participants(t *testing.T, room string) map[string]*livekit.ParticipantInfo {
	rsc := lksdk.NewRoomServiceClient(env(t, "E2E_LIVEKIT_HTTP", "http://127.0.0.1:7880"), env(t, "LIVEKIT_API_KEY", ""), env(t, "LIVEKIT_API_SECRET", ""))
	out := map[string]*livekit.ParticipantInfo{}
	if ps, err := rsc.ListParticipants(context.Background(), &livekit.ListParticipantsRequest{Room: room}); err == nil {
		for _, p := range ps.Participants {
			out[p.Identity] = p
		}
	}
	return out
}

func eventually(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", d, what)
}

func TestInboundWHIPUserHangup(t *testing.T) {
	st := startStub(t, env(t, "E2E_STUB_ADDR", "127.0.0.1:18080"))
	res, room, finish := runCall(t, "hangup", st, 0)
	defer finish()
	if got := st.list(); got != "pre_accept,accept" {
		t.Fatalf("Meta actions = %q, want pre_accept,accept", got)
	}
	eventually(t, "audio both ways", 15*time.Second, func() bool { return res.callerRx.Load() > 50 && res.agentRx.Load() > 50 })
	ps := participants(t, room)
	if c := ps["wa-e2e"]; c == nil || c.Kind != livekit.ParticipantInfo_CONNECTOR || len(c.KindDetails) == 0 || c.KindDetails[0] != livekit.ParticipantInfo_CONNECTOR_WHATSAPP || c.Attributes["k"] != "v" {
		t.Fatalf("caller participant not as the worker expects: %+v", c)
	}
	t.Logf("audio: caller<-agent=%d agent<-caller=%d", res.callerRx.Load(), res.agentRx.Load())
	finish()
	eventually(t, "caller removed from the room", 10*time.Second, func() bool { return participants(t, room)["wa-e2e"] == nil })
	if got := st.list(); got != "pre_accept,accept" {
		t.Fatalf("user hangup must not terminate at Meta, actions = %q", got)
	}
}

func TestInboundWHIPAgentLeavesTerminatesAtMeta(t *testing.T) {
	st := startStub(t, env(t, "E2E_STUB_ADDR", "127.0.0.1:18080"))
	_, _, finish := runCall(t, "agentleft", st, 3*time.Second)
	defer finish()
	eventually(t, "terminate sent to Meta after the agent left", 20*time.Second, func() bool { return st.list() == "pre_accept,accept,terminate" })
}
