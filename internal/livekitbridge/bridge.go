package livekitbridge

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"

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
	translator  rtpTranslator
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
	if err = room.JoinWithContextAndToken(ctx, conf.URL, token, lksdk.WithAutoSubscribe(true), lksdk.WithExtraAttributes(conf.Attributes)); err != nil {
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

func (b *Bridge) WriteFromMeta(p *rtp.Packet) error {
	if b.local == nil {
		return io.ErrClosedPipe
	}
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
		b.translator.rewrite(p, b.conf.PayloadType)
		if err = b.conf.WriteToMeta(p); err != nil {
			return
		}
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

type rtpTranslator struct {
	mu                   sync.Mutex
	initialized          bool
	ssrc                 uint32
	seq                  uint16
	timestamp, lastInput uint32
}

func (t *rtpTranslator) rewrite(p *rtp.Packet, pt uint8) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.initialized {
		t.initialized = true
		t.ssrc, t.seq, t.timestamp, t.lastInput = p.SSRC^0x6d2b79f5, p.SequenceNumber, p.Timestamp, p.Timestamp
	} else {
		delta := p.Timestamp - t.lastInput
		if delta == 0 || delta > 48000*2 {
			delta = 960
		}
		t.seq++
		t.timestamp += delta
		t.lastInput = p.Timestamp
	}
	p.PayloadType, p.SSRC, p.SequenceNumber, p.Timestamp = pt, t.ssrc, t.seq, t.timestamp
}
