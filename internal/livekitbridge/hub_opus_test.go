//go:build cgo

package livekitbridge

import (
	"bytes"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/livekit/media-sdk"
	"github.com/pion/rtp"
)

// tone returns n 20 ms Opus frames of a sine at freq Hz.
func tone(t *testing.T, freq float64, n int) [][]byte {
	t.Helper()
	var mu sync.Mutex
	var frames [][]byte
	enc, err := OpusCodec().NewEncoder(func(p []byte) error {
		mu.Lock()
		frames = append(frames, append([]byte(nil), p...))
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	for f := 0; f < n; f++ {
		s := make(media.PCM16Sample, 960)
		for i := range s {
			s[i] = int16(8000 * math.Sin(2*math.Pi*freq*float64(f*960+i)/48000))
		}
		if err := enc.WriteSample(s); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return frames
}

type pcmCollector struct {
	mu  sync.Mutex
	pcm []int16
}

func (c *pcmCollector) String() string  { return "collector" }
func (c *pcmCollector) SampleRate() int { return 48000 }
func (c *pcmCollector) Close() error    { return nil }
func (c *pcmCollector) WriteSample(s media.PCM16Sample) error {
	c.mu.Lock()
	c.pcm = append(c.pcm, s...)
	c.mu.Unlock()
	return nil
}

// power is the Goertzel energy of freq in pcm.
func power(pcm []int16, freq float64) float64 {
	w := 2 * math.Pi * freq / 48000
	coeff := 2 * math.Cos(w)
	var s1, s2 float64
	for _, v := range pcm {
		s0 := float64(v) + coeff*s1 - s2
		s2, s1 = s1, s0
	}
	return (s1*s1 + s2*s2 - coeff*s1*s2) / float64(len(pcm))
}

func decodeAll(t *testing.T, pkts []*rtp.Packet) []int16 {
	t.Helper()
	col := &pcmCollector{}
	dec, err := OpusCodec().NewDecoder(col)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	for _, p := range pkts {
		if err := dec.Decode(p.Payload); err != nil {
			t.Fatal(err)
		}
	}
	return col.pcm
}

func opusPacket(ssrc uint32, seq uint16, payload []byte) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{PayloadType: 96, SSRC: ssrc, SequenceNumber: seq, Timestamp: uint32(seq) * 960}, Payload: payload}
}

func TestRealOpusSingleSpeakerIsForwardedBitExact(t *testing.T) {
	frames := tone(t, 440, 25)
	out := &sink{}
	h := NewHub(HubConfig{Codec: OpusCodec(), PayloadType: 111, Emit: out.emit})
	defer h.Close()
	src := h.AddSource(levelReader{})
	for i, f := range frames {
		src.Push(opusPacket(1, uint16(i), append([]byte(nil), f...)))
	}
	got := out.all()
	if len(got) != len(frames) {
		t.Fatalf("forwarded %d of %d frames", len(got), len(frames))
	}
	for i := range got {
		if !bytes.Equal(got[i].Payload, frames[i]) {
			t.Fatalf("frame %d was re-encoded", i)
		}
	}
	if s := h.Stats(); s.Episodes != 0 {
		t.Fatal("mixed a single speaker")
	}
}

func TestRealOpusOverlapCarriesBothSpeakers(t *testing.T) {
	const n = 100 // 2 s
	low, high := tone(t, 440, n), tone(t, 1250, n)
	out := &sink{}
	h := NewHub(HubConfig{Codec: OpusCodec(), PayloadType: 111, Emit: out.emit})
	defer h.Close()
	a, b := h.AddSource(levelReader{}), h.AddSource(levelReader{})
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for i := 0; i < n; i++ {
		<-tick.C
		a.Push(opusPacket(1, uint16(i), append([]byte(nil), low[i]...)))
		b.Push(opusPacket(2, uint16(i), append([]byte(nil), high[i]...)))
	}
	time.Sleep(100 * time.Millisecond)
	if h.Stats().Episodes != 1 {
		t.Fatalf("episodes = %d, want 1", h.Stats().Episodes)
	}
	pcm := decodeAll(t, out.all())
	if len(pcm) < 48000 {
		t.Fatalf("only %d samples came out", len(pcm))
	}
	pcm = pcm[len(pcm)/2:] // steady state, after mixer buffering
	pl, ph, pn := power(pcm, 440), power(pcm, 1250), power(pcm, 3000)
	if pl < 100*pn || ph < 100*pn {
		t.Fatalf("both tones should be present: 440Hz=%.0f 1250Hz=%.0f noise=%.0f", pl, ph, pn)
	}
	// the stream Meta sees is one continuous RTP stream
	pk := out.all()
	for i := 1; i < len(pk); i++ {
		if pk[i].SSRC != pk[0].SSRC {
			t.Fatalf("SSRC changed at packet %d", i)
		}
	}
}
