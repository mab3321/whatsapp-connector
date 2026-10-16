package livekitbridge

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"

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
	outbound    rtpRebaser // LiveKit -> Meta
	inbound     rtpRebaser // Meta -> LiveKit

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
	cb := &lksdk.RoomCallback{
		OnDisconnected: func() {
			b.closeDone()
			if conf.OnDisconnected != nil {
				go conf.OnDisconnected()
			}
		},
		OnParticipantDisconnected: func(participant *lksdk.RemoteParticipant) {
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
				if pub.Kind() == lksdk.TrackKindAudio {
					_ = pub.SetSubscribed(true)
				}
			},
			OnTrackSubscribed: func(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, _ *lksdk.RemoteParticipant) {
				if strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
					go b.forwardRemote(track, pub.SID())
				}
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
		lksdk.WithAutoSubscribe(true),
		lksdk.WithExtraAttributes(conf.Attributes),
		lksdk.WithCodecs([]livekit.Codec{{Mime: webrtc.MimeTypeOpus}}),
	); err != nil {
		return nil, err
	}
	b.room = room
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
	if b.room != nil {
		b.room.Disconnect()
	}
	return nil
}
