package livekitbridge

import (
	"sync"
	"testing"
	"time"

	"github.com/livekit/media-sdk"
	"github.com/pion/rtp"
)

// fakeCodec "decodes" a payload into 20 ms of a constant sample (its first byte)
// and "encodes" a mixed frame into a payload holding the first sample.
type fakeCodec struct {
	mu                 sync.Mutex
	decoders, encoders int
	decodeCalls        int
	closedDecoders     int
}

func (c *fakeCodec) NewDecoder(out media.PCM16Writer) (Decoder, error) {
	c.mu.Lock()
	c.decoders++
	c.mu.Unlock()
	return &fakeDecoder{c: c, out: out}, nil
}

func (c *fakeCodec) NewEncoder(sink func([]byte) error) (media.PCM16Writer, error) {
	c.mu.Lock()
	c.encoders++
	c.mu.Unlock()
	return &fakeEncoder{sink: sink}, nil
}

func (c *fakeCodec) counts() (decoders, encoders, calls, closed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.decoders, c.encoders, c.decodeCalls, c.closedDecoders
}

type fakeDecoder struct {
	c   *fakeCodec
	out media.PCM16Writer
}

func (d *fakeDecoder) Decode(payload []byte) error {
	d.c.mu.Lock()
	d.c.decodeCalls++
	d.c.mu.Unlock()
	s := make(media.PCM16Sample, 960)
	for i := range s {
		s[i] = int16(payload[0])
	}
	return d.out.WriteSample(s)
}

func (d *fakeDecoder) Close() error {
	d.c.mu.Lock()
	d.c.closedDecoders++
	d.c.mu.Unlock()
	return nil
}

type fakeEncoder struct{ sink func([]byte) error }

func (e *fakeEncoder) String() string  { return "fake encoder" }
func (e *fakeEncoder) SampleRate() int { return 48000 }
func (e *fakeEncoder) Close() error    { return nil }
func (e *fakeEncoder) WriteSample(s media.PCM16Sample) error {
	return e.sink([]byte{byte(s[0]), 0, 0, 0})
}

type sink struct {
	mu   sync.Mutex
	pkts []*rtp.Packet
}

func (s *sink) emit(p *rtp.Packet) error {
	s.mu.Lock()
	s.pkts = append(s.pkts, p)
	s.mu.Unlock()
	return nil
}

func (s *sink) all() []*rtp.Packet {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*rtp.Packet(nil), s.pkts...)
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestHub(codec Codec) (*Hub, *sink, *clock) {
	out, clk := &sink{}, &clock{t: time.Unix(1000, 0)}
	h := NewHub(HubConfig{Codec: codec, PayloadType: 111, Emit: out.emit, Now: clk.now})
	return h, out, clk
}

// speech is a packet the hub treats as audible, silence as DTX.
func speech(ssrc uint32, seq uint16, level byte) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{PayloadType: 96, SSRC: ssrc, SequenceNumber: seq, Timestamp: uint32(seq) * 960},
		Payload: []byte{level, 1, 2, 3, 4, 5}}
}

func silence(ssrc uint32, seq uint16) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{PayloadType: 96, SSRC: ssrc, SequenceNumber: seq, Timestamp: uint32(seq) * 960},
		Payload: []byte{0xf8, 0xff}}
}

func TestSingleSpeakerIsForwardedWithoutTouchingTheCodec(t *testing.T) {
	codec := &fakeCodec{}
	h, out, clk := newTestHub(codec)
	defer h.Close()
	agent := h.AddSource(levelReader{})
	for i := 0; i < 20; i++ {
		agent.Push(speech(1, uint16(i), 5))
		clk.advance(20 * time.Millisecond)
	}
	pkts := out.all()
	if len(pkts) != 20 {
		t.Fatalf("forwarded %d packets, want 20", len(pkts))
	}
	if pkts[0].PayloadType != 111 || pkts[0].Payload[0] != 5 || len(pkts[0].Payload) != 6 {
		t.Fatalf("packet was altered: %+v", pkts[0])
	}
	if d, e, _, _ := codec.counts(); d != 0 || e != 0 {
		t.Fatalf("codec used for a single speaker: %d decoders, %d encoders", d, e)
	}
	if s := h.Stats(); s.Passthrough != 20 || s.Episodes != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestSilentTrackIsDroppedNotDecodedWhileAnotherSpeaks(t *testing.T) {
	codec := &fakeCodec{}
	h, out, clk := newTestHub(codec)
	defer h.Close()
	agent, human := h.AddSource(levelReader{}), h.AddSource(levelReader{})
	for i := 0; i < 10; i++ {
		agent.Push(speech(1, uint16(i), 9))
		human.Push(silence(2, uint16(i)))
		clk.advance(20 * time.Millisecond)
	}
	if got := len(out.all()); got != 10 {
		t.Fatalf("forwarded %d, want only the agent's 10", got)
	}
	if d, e, _, _ := codec.counts(); d != 0 || e != 0 {
		t.Fatalf("codec used: %d decoders, %d encoders", d, e)
	}
}

func TestSpeechFromAnotherTrackTakesTheLineWithoutMixing(t *testing.T) {
	codec := &fakeCodec{}
	h, out, clk := newTestHub(codec)
	defer h.Close()
	agent, human := h.AddSource(levelReader{}), h.AddSource(levelReader{})
	agent.Push(speech(1, 1, 7))
	clk.advance(time.Second) // agent has stopped talking
	human.Push(speech(2, 1, 8))
	agent.Push(silence(1, 2))
	pkts := out.all()
	if len(pkts) != 2 || pkts[1].Payload[0] != 8 {
		t.Fatalf("human did not take the line: %d packets", len(pkts))
	}
	if pkts[0].SSRC != pkts[1].SSRC || pkts[1].SequenceNumber != pkts[0].SequenceNumber+1 {
		t.Fatalf("stream is not continuous across the change: %+v then %+v", pkts[0].Header, pkts[1].Header)
	}
	if d, _, _, _ := codec.counts(); d != 0 {
		t.Fatal("decoded although the speakers did not overlap")
	}
}

func TestQuietLineKeepsTheLastSpeakersStreamFlowing(t *testing.T) {
	h, out, clk := newTestHub(&fakeCodec{})
	defer h.Close()
	agent, human := h.AddSource(levelReader{}), h.AddSource(levelReader{})
	agent.Push(speech(1, 1, 7))
	clk.advance(time.Second)
	agent.Push(silence(1, 2))
	human.Push(silence(2, 1))
	if got := len(out.all()); got != 2 {
		t.Fatalf("forwarded %d packets, want the agent's speech and silence frames", got)
	}
}

func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestOverlappingSpeakersAreDecodedMixedAndEncodedOnce(t *testing.T) {
	codec := &fakeCodec{}
	h, out, clk := newTestHub(codec)
	defer h.Close()
	agent, human := h.AddSource(levelReader{}), h.AddSource(levelReader{})
	silent := h.AddSource(levelReader{})
	defer silent.Close()

	feed := func() {
		for i := 0; i < 12; i++ {
			agent.Push(speech(1, uint16(i), 10))
			human.Push(speech(2, uint16(i), 20))
			silent.Push(silence(3, uint16(i)))
			time.Sleep(20 * time.Millisecond)
		}
	}
	feed()
	waitUntil(t, "mixed frames", func() bool { return h.Stats().Mixed > 0 })

	if !h.Mixing() {
		t.Fatal("not mixing during an overlap")
	}
	d, e, _, _ := codec.counts()
	if d != 2 || e != 1 {
		t.Fatalf("%d decoders and %d encoders, want 2 and 1 (the silent track must not be decoded)", d, e)
	}
	var summed bool
	for _, p := range out.all() {
		if len(p.Payload) == 4 && p.Payload[0] == 30 {
			summed = true
		}
	}
	if !summed {
		t.Fatal("no mixed frame carries the sum of both speakers")
	}
	_ = clk
}

func TestMixingEndsAfterTheOverlapAndReleasesTheDecoders(t *testing.T) {
	codec := &fakeCodec{}
	h, out, clk := newTestHub(codec)
	defer h.Close()
	agent, human := h.AddSource(levelReader{}), h.AddSource(levelReader{})
	for i := 0; i < 8; i++ {
		agent.Push(speech(1, uint16(i), 10))
		human.Push(speech(2, uint16(i), 20))
		time.Sleep(20 * time.Millisecond)
	}
	waitUntil(t, "mixing to start", h.Mixing)

	clk.advance(3 * time.Second) // everyone paused well past both hangovers
	agent.Push(speech(1, 100, 11))
	if h.Mixing() {
		t.Fatal("still mixing after the overlap ended")
	}
	_, _, _, closed := codec.counts()
	if closed != 2 {
		t.Fatalf("%d decoders closed, want 2", closed)
	}
	before := len(out.all())
	agent.Push(speech(1, 101, 12))
	pkts := out.all()
	if len(pkts) != before+1 || pkts[len(pkts)-1].Payload[0] != 12 || len(pkts[len(pkts)-1].Payload) != 6 {
		t.Fatal("the remaining speaker is not forwarded untouched")
	}
}

func TestOverlapWithoutACodecKeepsTheFirstSpeaker(t *testing.T) {
	h, out, clk := newTestHub(nil)
	defer h.Close()
	agent, human := h.AddSource(levelReader{}), h.AddSource(levelReader{})
	for i := 0; i < 5; i++ {
		agent.Push(speech(1, uint16(i), 10))
		human.Push(speech(2, uint16(i), 20))
		clk.advance(20 * time.Millisecond)
	}
	pkts := out.all()
	if len(pkts) != 5 {
		t.Fatalf("forwarded %d packets, want only the first speaker's 5", len(pkts))
	}
	for _, p := range pkts {
		if p.Payload[0] != 10 {
			t.Fatal("the second speaker leaked into the stream")
		}
	}
}

func TestClosedSourceStopsAndHubCloseIsSafe(t *testing.T) {
	h, out, _ := newTestHub(&fakeCodec{})
	a := h.AddSource(levelReader{})
	a.Close()
	a.Push(speech(1, 1, 1))
	h.Close()
	h.Close()
	if len(out.all()) != 0 {
		t.Fatal("a closed source still forwarded audio")
	}
}
