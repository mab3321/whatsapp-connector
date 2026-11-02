package livekitbridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lkconnector "github.com/livekit/protocol/connector"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type Config struct {
	URL, APIKey, APISecret             string
	RoomName, Identity, Name, Metadata string
	Attributes                         map[string]string
	Agents                             []*livekit.RoomAgentDispatch
	PayloadType                        uint8
	WriteToMeta                        func(*rtp.Packet) error
	OnDisconnected                     func()
	// MixAudio lets several participants be heard at once: a single speaker is
	// forwarded untouched and only an overlap is decoded, mixed and re-encoded.
	// Off keeps the earlier behaviour (the first track wins).
	MixAudio bool
	// Codec performs the decoding/encoding of an overlap; nil disables mixing.
	Codec Codec
	// DeferAudio keeps the bridge from subscribing to the room's audio until
	// ReleaseAudio is called. An outbound call uses it so the agent can load and
	// generate its greeting while the phone rings: the agent's audio output holds
	// the greeting until something subscribes, and the callee hears it in full the
	// moment the call is answered.
	DeferAudio bool
	// EmptyGrace is how long the room may have no audio publisher before the
	// call ends (MixAudio only). Zero uses a default.
	EmptyGrace time.Duration
}

type Bridge struct {
	log         *slog.Logger
	conf        Config
	room        *lksdk.Room
	local       *webrtc.TrackLocalStaticRTP
	done        chan struct{}
	closeOnce   sync.Once
	trackMu     sync.Mutex
	activeTrack string
	hub         *Hub // set when MixAudio is on
	presence    *presence
	roomReady   atomic.Bool
	audioOpen   atomic.Bool // subscribing to remote audio is allowed
	outbound    rtpRebaser  // LiveKit -> Meta
	inbound     rtpRebaser  // Meta -> LiveKit

	txPackets, txDropped, txWriteErrors atomic.Uint64
	rxPackets                           atomic.Uint64
}

// Stats are cumulative counters for one bridge.
type Stats struct {
	TxPackets, TxDropped, TxWriteErrors, RxPackets uint64
}

func (b *Bridge) Stats() Stats {
	return Stats{b.txPackets.Load(), b.txDropped.Load(), b.txWriteErrors.Load(), b.rxPackets.Load()}
}

func Connect(ctx context.Context, log *slog.Logger, conf Config) (*Bridge, error) {
	b := &Bridge{log: log, conf: conf, done: make(chan struct{})}
	b.audioOpen.Store(!conf.DeferAudio)
	if conf.MixAudio {
		b.hub = NewHub(HubConfig{Codec: conf.Codec, PayloadType: conf.PayloadType, Emit: b.emit, Log: log})
		b.presence = newPresence(conf.EmptyGrace, func() {
			b.log.Info("no participant is publishing audio any more; ending the call")
			b.closeDone()
			if conf.OnDisconnected != nil {
				go conf.OnDisconnected()
			}
		})
	}
	cb := &lksdk.RoomCallback{
		OnDisconnected: func() {
			b.closeDone()
			if conf.OnDisconnected != nil {
				go conf.OnDisconnected()
			}
		},
		OnParticipantConnected: func(*lksdk.RemoteParticipant) { b.checkPresence() },
		OnParticipantDisconnected: func(participant *lksdk.RemoteParticipant) {
			if b.presence != nil {
				b.checkPresence()
				return
			}
			if !endsCallOnDeparture(participant.Kind()) {
				return
			}
			b.log.Info("agent participant disconnected", "participant", participant.Identity())
			b.closeDone()
			if conf.OnDisconnected != nil {
				go conf.OnDisconnected()
			}
		},
		ParticipantCallback: lksdk.ParticipantCallback{
			OnTrackPublished: func(pub *lksdk.RemoteTrackPublication, _ *lksdk.RemoteParticipant) {
				if pub.Kind() == lksdk.TrackKindAudio && b.audioOpen.Load() {
					_ = pub.SetSubscribed(true)
				}
				b.checkPresence()
			},
			OnTrackUnpublished: func(*lksdk.RemoteTrackPublication, *lksdk.RemoteParticipant) { b.checkPresence() },
			OnTrackSubscribed: func(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, _ *lksdk.RemoteParticipant) {
				if !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
					return
				}
				if b.hub != nil {
					go b.pump(track, pub)
					return
				}
				go b.forwardRemote(track, pub.SID())
			},
			OnTrackUnsubscribed: func(_ *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, _ *lksdk.RemoteParticipant) {
				b.trackMu.Lock()
				if b.activeTrack == pub.SID() {
					b.activeTrack = ""
				}
				b.trackMu.Unlock()
			},
		},
	}
	token, err := lkconnector.BuildConnectorToken(lkconnector.ConnectorTokenParams{
		APIKey: conf.APIKey, APISecret: conf.APISecret, RoomName: conf.RoomName,
		ParticipantIdentity: conf.Identity, ParticipantName: conf.Name,
		ParticipantMetadata: conf.Metadata, ParticipantAttributes: conf.Attributes,
		Agents: conf.Agents, Kind: lkconnector.WHATSAPP,
	})
	if err != nil {
		return nil, err
	}
	room := lksdk.NewRoom(cb)
	if err = room.JoinWithContextAndToken(ctx, conf.URL, token,
		lksdk.WithAutoSubscribe(!conf.DeferAudio),
		lksdk.WithExtraAttributes(conf.Attributes),
		lksdk.WithCodecs([]livekit.Codec{{Mime: webrtc.MimeTypeOpus}}),
	); err != nil {
		return nil, err
	}
	b.room = room
	b.roomReady.Store(true)
	b.checkPresence()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "whatsapp-audio", "whatsapp")
	if err != nil {
		room.Disconnect()
		return nil, err
	}
	if _, err = room.LocalParticipant.PublishTrack(track, &lksdk.TrackPublicationOptions{Name: "whatsapp-audio", Source: livekit.TrackSource_MICROPHONE}); err != nil {
		room.Disconnect()
		return nil, err
	}
	b.local = track
	return b, nil
}

func endsCallOnDeparture(kind lksdk.ParticipantKind) bool {
	return kind == lksdk.ParticipantAgent
}

func (b *Bridge) WriteFromMeta(p *rtp.Packet) error {
	if b.local == nil {
		return io.ErrClosedPipe
	}
	b.rxPackets.Add(1)
	// Keep Meta's sequence numbers, but stay continuous if Meta changes SSRC
	// mid-call (for example after a network change).
	b.inbound.keepExtensions = true
	b.inbound.rewrite(p, p.PayloadType)
	return b.local.WriteRTP(p)
}

func (b *Bridge) forwardRemote(track *webrtc.TrackRemote, id string) {
	b.trackMu.Lock()
	if b.activeTrack != "" {
		b.trackMu.Unlock()
		return
	}
	b.activeTrack = id
	b.trackMu.Unlock()
	defer func() {
		b.trackMu.Lock()
		if b.activeTrack == id {
			b.activeTrack = ""
		}
		b.trackMu.Unlock()
	}()
	for {
		p, _, err := track.ReadRTP()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				b.log.Warn("LiveKit audio track ended", "error", err)
			}
			return
		}
		b.outbound.rewrite(p, b.conf.PayloadType)
		if err = b.conf.WriteToMeta(p); err != nil {
			if isFatalWrite(err) {
				return
			}
			// A transient failure (Meta link not ready yet, a send error on a
			// flaky network) must cost one packet, not the rest of the call's
			// agent audio.
			b.countWriteFailure(err)
			continue
		}
		b.txPackets.Add(1)
	}
}

// isFatalWrite reports errors that mean the media path is gone for good.
func isFatalWrite(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// ErrNotReady is returned by WriteToMeta while the Meta media path is still
// being established; the packet is dropped rather than queued so stale audio
// is never delivered in a burst.
var ErrNotReady = errors.New("meta media path not ready")

func (b *Bridge) countWriteFailure(err error) {
	if errors.Is(err, ErrNotReady) {
		b.txDropped.Add(1)
		return
	}
	n := b.txWriteErrors.Add(1)
	if n == 1 || n%500 == 0 {
		b.log.Warn("audio write to Meta failed; continuing", "error", err, "failures", n)
	}
}

func (b *Bridge) Done() <-chan struct{} { return b.done }
func (b *Bridge) closeDone()            { b.closeOnce.Do(func() { close(b.done) }) }
func (b *Bridge) Close() error {
	b.closeDone()
	if b.hub != nil {
		b.hub.Close()
	}
	if b.presence != nil {
		b.presence.stop()
	}
	if b.room != nil {
		b.room.Disconnect()
	}
	return nil
}

// emit sends one packet of the merged stream to Meta, counting what happens to it.
func (b *Bridge) emit(p *rtp.Packet) error {
	if err := b.conf.WriteToMeta(p); err != nil {
		if !isFatalWrite(err) {
			b.countWriteFailure(err)
		}
		return err
	}
	b.txPackets.Add(1)
	return nil
}

// pump feeds one subscribed track into the hub.
func (b *Bridge) pump(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication) {
	src := b.hub.AddSource(levelReaderFor(pub.Receiver()))
	defer src.Close()
	first := true
	for {
		p, _, err := track.ReadRTP()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				b.log.Warn("LiveKit audio track ended", "error", err)
			}
			return
		}
		if first {
			first = false
			var offered []string
			if r := pub.Receiver(); r != nil {
				for _, e := range r.GetParameters().HeaderExtensions {
					offered = append(offered, fmt.Sprintf("%d=%s", e.ID, e.URI))
				}
			}
			var seen []string
			for _, id := range p.GetExtensionIDs() {
				seen = append(seen, fmt.Sprintf("%d:%dB", id, len(p.GetExtension(id))))
			}
			b.log.Info("audio track attached", "kind", pub.Kind(), "negotiated_extensions", offered, "packet_extensions", seen, "payload_bytes", len(p.Payload))
		}
		src.Push(p)
	}
}

// ReleaseAudio starts listening to the room: it subscribes to every audio track
// that is published now and to those published later. It is a no-op unless the
// bridge was created with DeferAudio.
func (b *Bridge) ReleaseAudio() {
	if b.hub != nil {
		b.hub.Mark()
	}
	if b.audioOpen.Swap(true) || b.room == nil {
		return
	}
	for _, rp := range b.room.GetRemoteParticipants() {
		for _, pub := range rp.TrackPublications() {
			if remote, ok := pub.(*lksdk.RemoteTrackPublication); ok && remote.Kind() == lksdk.TrackKindAudio {
				_ = remote.SetSubscribed(true)
			}
		}
	}
}

// HubStats reports the mixing hub counters (zero when mixing is off).
func (b *Bridge) HubStats() HubStats {
	if b.hub == nil {
		return HubStats{}
	}
	return b.hub.Stats()
}

// checkPresence re-evaluates who is still publishing audio in the room.
func (b *Bridge) checkPresence() {
	if b.presence == nil || !b.roomReady.Load() {
		return
	}
	audible := 0
	for _, rp := range b.room.GetRemoteParticipants() {
		if rp.Kind() == lksdk.ParticipantEgress {
			continue
		}
		for _, pub := range rp.TrackPublications() {
			if pub.Kind() == lksdk.TrackKindAudio {
				audible++
				break
			}
		}
	}
	b.presence.update(audible)
}
