package livekitbridge

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/mixer"
	"github.com/pion/rtp"
)

const (
	mixFrame = 20 * time.Millisecond
	// A source counts as live for this long after its last speech packet, so
	// the gaps between words do not flip the hub between modes.
	speechHangover = 400 * time.Millisecond
	// Mixing continues this long after the overlap ends before the hub goes
	// back to forwarding the remaining speaker untouched.
	mixHangover = time.Second
	mixSSRCBase = 0x4d495800
)

// HubConfig wires a Hub to the Meta leg.
type HubConfig struct {
	// Codec is optional. Without it overlapping speakers are not mixed: the
	// first one keeps the line.
	Codec       Codec
	PayloadType uint8
	// Emit sends one packet towards Meta. It must not block.
	Emit func(*rtp.Packet) error
	Log  *slog.Logger
	// Now is overridable for tests.
	Now func() time.Time
}

// HubStats are cumulative counters.
type HubStats struct {
	Passthrough, Mixed, Dropped, Episodes, CodecErrors uint64
}

// Hub merges the audio tracks of a room into the single stream Meta receives.
//
// It picks the cheapest path that is still correct:
//
//   - nobody speaking: the last speaker's packets keep flowing (comfort audio),
//     untouched;
//   - one speaker: that track's Opus packets are forwarded as they are, with no
//     decode, mix or encode;
//   - two or more speakers at once: only those tracks are decoded, mixed and
//     encoded once, and only while the overlap lasts (plus a short hangover).
//
// Tracks that are silent are never decoded.
type Hub struct {
	conf HubConfig

	mu      sync.Mutex
	sources map[*Source]struct{}
	primary *Source
	mix     *mixSession
	hold    time.Time // end of the mixing hangover
	closed  bool
	episode uint32
	epoch   time.Time // start of the call for timing logs; moved on pickup
	spoke   bool      // first speech since epoch has been logged

	outMu  sync.Mutex // serialises everything that reaches Emit and the rebaser
	rebase rtpRebaser

	passthrough, mixed, dropped, episodes, codecErrors atomic.Uint64
}

// NewHub creates a hub.
func NewHub(conf HubConfig) *Hub {
	if conf.Now == nil {
		conf.Now = time.Now
	}
	if conf.Log == nil {
		conf.Log = slog.Default()
	}
	h := &Hub{conf: conf, sources: map[*Source]struct{}{}, epoch: conf.Now()}
	h.rebase.now = conf.Now
	return h
}

// Stats returns the cumulative counters.
func (h *Hub) Stats() HubStats {
	return HubStats{h.passthrough.Load(), h.mixed.Load(), h.dropped.Load(), h.episodes.Load(), h.codecErrors.Load()}
}

// Mark restarts the clock used for the "first speech" timing, for example when
// the callee answers.
func (h *Hub) Mark() {
	h.mu.Lock()
	h.epoch, h.spoke = h.conf.Now(), false
	h.mu.Unlock()
}

// Mixing reports whether an overlap is currently being mixed.
func (h *Hub) Mixing() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mix != nil
}

// Source is one subscribed audio track.
type Source struct {
	h          *Hub
	level      levelReader
	lastSpeech time.Time
	dec        Decoder
	in         *mixer.Input
	closed     bool
}

// AddSource registers a track. Call Push for each of its packets and Close when
// the track ends.
func (h *Hub) AddSource(level levelReader) *Source {
	s := &Source{h: h, level: level}
	h.mu.Lock()
	if !h.closed {
		h.sources[s] = struct{}{}
		if h.primary == nil {
			h.primary = s
		}
	}
	h.mu.Unlock()
	return s
}

func (s *Source) live(now time.Time) bool {
	return !s.lastSpeech.IsZero() && now.Sub(s.lastSpeech) < speechHangover
}

// Close removes the track.
func (s *Source) Close() {
	h := s.h
	h.mu.Lock()
	defer h.mu.Unlock()
	s.closed = true
	s.releaseLocked()
	delete(h.sources, s)
	if h.primary == s {
		h.primary = nil
	}
}

func (s *Source) releaseLocked() {
	if s.dec != nil {
		_ = s.dec.Close()
		s.dec = nil
	}
	if s.in != nil {
		_ = s.in.Close()
		s.in = nil
	}
}

// Push handles one RTP packet of the track.
func (s *Source) Push(p *rtp.Packet) {
	h := s.h
	now := h.conf.Now()
	h.mu.Lock()
	if h.closed || s.closed {
		h.mu.Unlock()
		return
	}
	if s.level.speech(p) {
		s.lastSpeech = now
		if !h.spoke {
			h.spoke = true
			h.conf.Log.Info("first speech from the room", "after_start_ms", now.Sub(h.epoch).Milliseconds(), "audio_level_ext", s.level.id != 0)
		}
	}
	live := h.liveCountLocked(now)

	if h.mix != nil {
		if live >= 2 {
			h.hold = now.Add(mixHangover)
		} else if now.After(h.hold) {
			h.stopMixLocked()
		}
	} else if live >= 2 && s.live(now) && h.conf.Codec != nil {
		h.startMixLocked(now)
	}

	if h.mix != nil {
		if s.live(now) {
			h.decodeLocked(s, p)
		} else {
			h.dropped.Add(1)
		}
		h.mu.Unlock()
		return
	}

	forward := h.forwardsLocked(s, now, live)
	h.mu.Unlock()
	if !forward {
		h.dropped.Add(1)
		return
	}
	h.passthrough.Add(1)
	h.outMu.Lock()
	h.rebase.rewrite(p, h.conf.PayloadType)
	_ = h.conf.Emit(p)
	h.outMu.Unlock()
}

func (h *Hub) liveCountLocked(now time.Time) int {
	n := 0
	for s := range h.sources {
		if s.live(now) {
			n++
		}
	}
	return n
}

// forwardsLocked decides whether s owns the line while no mixing is going on.
func (h *Hub) forwardsLocked(s *Source, now time.Time, live int) bool {
	if live == 0 {
		// Nobody is speaking: keep the last speaker's stream going so Meta keeps
		// receiving the audio (including silence frames) it always did.
		if h.primary == nil {
			h.primary = s
		}
		return h.primary == s
	}
	if !s.live(now) {
		return false
	}
	if live == 1 || h.primary == nil || !h.primary.live(now) {
		h.primary = s
		return true
	}
	// Overlap without a codec: the speaker that already had the line keeps it.
	return h.primary == s
}

func (h *Hub) decodeLocked(s *Source, p *rtp.Packet) {
	if s.dec == nil {
		in := h.mix.mx.NewInput()
		if in == nil {
			return
		}
		dec, err := h.conf.Codec.NewDecoder(in)
		if err != nil {
			_ = in.Close()
			if h.codecErrors.Add(1) == 1 {
				h.conf.Log.Warn("cannot create Opus decoder", "error", err)
			}
			return
		}
		s.in, s.dec = in, dec
	}
	if err := s.dec.Decode(p.Payload); err != nil {
		if h.codecErrors.Add(1) == 1 {
			h.conf.Log.Warn("Opus decode failed", "error", err)
		}
	}
}

// mixSession is one overlap: a mixer feeding one encoder.
type mixSession struct {
	mx   *mixer.Mixer
	enc  *guardedPCM
	ssrc uint32
	seq  uint16
	ts   uint32
	dead bool // guarded by Hub.outMu
}

func (h *Hub) startMixLocked(now time.Time) {
	h.episode++
	sess := &mixSession{ssrc: mixSSRCBase + h.episode, seq: uint16(h.episode * 7919), ts: h.episode * 48000}
	enc, err := h.conf.Codec.NewEncoder(func(payload []byte) error { return h.emitMixed(sess, payload) })
	if err != nil {
		if h.codecErrors.Add(1) == 1 {
			h.conf.Log.Warn("cannot create Opus encoder", "error", err)
		}
		return
	}
	sess.enc = &guardedPCM{w: enc}
	mx, err := mixer.NewMixer(sess.enc, mixFrame, 1, mixer.WithOutputChannel())
	if err != nil {
		_ = enc.Close()
		h.codecErrors.Add(1)
		return
	}
	sess.mx = mx
	h.mix = sess
	h.hold = now.Add(mixHangover)
	h.episodes.Add(1)
	h.conf.Log.Info("mixing overlapping audio tracks")
}

func (h *Hub) stopMixLocked() {
	sess := h.mix
	h.mix = nil
	for s := range h.sources {
		s.releaseLocked()
	}
	sess.mx.Stop()
	h.outMu.Lock()
	sess.dead = true
	h.outMu.Unlock()
	sess.enc.Close()
	h.conf.Log.Info("back to forwarding a single audio track")
}

func (h *Hub) emitMixed(sess *mixSession, payload []byte) error {
	h.outMu.Lock()
	defer h.outMu.Unlock()
	if sess.dead {
		return nil
	}
	p := &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: h.conf.PayloadType, SequenceNumber: sess.seq, Timestamp: sess.ts, SSRC: sess.ssrc},
		Payload: append([]byte(nil), payload...),
	}
	sess.seq++
	sess.ts += opusFrameTicks
	h.rebase.rewrite(p, h.conf.PayloadType)
	h.mixed.Add(1)
	return h.conf.Emit(p)
}

// Close stops mixing and detaches every source.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	if h.mix != nil {
		h.stopMixLocked()
	}
	for s := range h.sources {
		s.closed = true
		s.releaseLocked()
	}
	h.sources = map[*Source]struct{}{}
}

// guardedPCM makes closing an encoder safe while the mixer may still be writing.
type guardedPCM struct {
	mu     sync.Mutex
	w      media.PCM16Writer
	closed bool
}

func (g *guardedPCM) String() string  { return g.w.String() }
func (g *guardedPCM) SampleRate() int { return g.w.SampleRate() }
func (g *guardedPCM) WriteSample(s media.PCM16Sample) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	return g.w.WriteSample(s)
}
func (g *guardedPCM) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	g.closed = true
	return g.w.Close()
}
