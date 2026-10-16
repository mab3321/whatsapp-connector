package livekitbridge

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newRebaser() (*rtpRebaser, *fakeClock) {
	c := &fakeClock{t: time.Unix(1000, 0)}
	return &rtpRebaser{now: c.now}, c
}

func pkt(ssrc uint32, seq uint16, ts uint32) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 100, SSRC: ssrc, SequenceNumber: seq, Timestamp: ts}}
}

func feed(r *rtpRebaser, c *fakeClock, ssrc uint32, in [][2]uint32) (seqs []uint16, tss []uint32) {
	for _, x := range in {
		p := pkt(ssrc, uint16(x[0]), x[1])
		r.rewrite(p, 111)
		c.advance(20 * time.Millisecond)
		seqs = append(seqs, p.SequenceNumber)
		tss = append(tss, p.Timestamp)
	}
	return
}

func TestRebaserPreservesLossAsSequenceGap(t *testing.T) {
	r, c := newRebaser()
	seqs, tss := feed(r, c, 7, [][2]uint32{{1, 960}, {2, 1920}, {4, 3840}, {5, 4800}})
	if seqs[2]-seqs[1] != 2 || tss[2]-tss[1] != 1920 {
		t.Fatalf("a lost packet must stay visible: seq=%v ts=%v", seqs, tss)
	}
}

func TestRebaserKeepsReorderingWithoutShiftingTimeline(t *testing.T) {
	r, c := newRebaser()
	seqs, tss := feed(r, c, 7, [][2]uint32{{1, 960}, {2, 1920}, {4, 3840}, {3, 2880}, {5, 4800}, {6, 5760}})
	// Output differs from input by one constant offset, so later packets are
	// not pushed ahead by the earlier reordering.
	offS, offT := seqs[0]-1, tss[0]-960
	in := [][2]uint32{{1, 960}, {2, 1920}, {4, 3840}, {3, 2880}, {5, 4800}, {6, 5760}}
	for i, x := range in {
		if seqs[i] != uint16(x[0])+offS || tss[i] != x[1]+offT {
			t.Fatalf("packet %d: got (%d,%d), want (%d,%d)", i, seqs[i], tss[i], uint16(x[0])+offS, x[1]+offT)
		}
	}
}

func TestRebaserKeepsDuplicatesIdentical(t *testing.T) {
	r, c := newRebaser()
	seqs, tss := feed(r, c, 7, [][2]uint32{{1, 960}, {2, 1920}, {2, 1920}, {3, 2880}})
	if seqs[1] != seqs[2] || tss[1] != tss[2] {
		t.Fatalf("a duplicate must stay a duplicate so the receiver drops it: %v %v", seqs, tss)
	}
}

func TestRebaserHandlesSequenceWraparound(t *testing.T) {
	r, c := newRebaser()
	seqs, _ := feed(r, c, 7, [][2]uint32{{65534, 100}, {65535, 1060}, {0, 2020}, {1, 2980}})
	for i := 1; i < len(seqs); i++ {
		if seqs[i]-seqs[i-1] != 1 {
			t.Fatalf("wraparound broke continuity: %v", seqs)
		}
	}
}

func TestRebaserContinuesAcrossSSRCChange(t *testing.T) {
	r, c := newRebaser()
	seqs, tss := feed(r, c, 7, [][2]uint32{{10, 5000}, {11, 5960}, {12, 6920}})
	c.advance(100 * time.Millisecond) // a short silence, then a new source with unrelated numbering
	p := pkt(99, 40000, 123456)
	r.rewrite(p, 111)
	if p.SSRC != r.outSSRC {
		t.Fatalf("output SSRC must stay constant")
	}
	if p.SequenceNumber != seqs[2]+1 {
		t.Fatalf("sequence must continue: last %d, got %d", seqs[2], p.SequenceNumber)
	}
	if got := p.Timestamp - tss[2]; got < opusFrameTicks || got > 8*opusFrameTicks {
		t.Fatalf("timestamp must advance by about the elapsed time, advanced %d ticks", got)
	}
	// Packets of the new source keep their own spacing afterwards.
	q := pkt(99, 40001, 123456+960)
	r.rewrite(q, 111)
	if q.SequenceNumber != p.SequenceNumber+1 || q.Timestamp != p.Timestamp+960 {
		t.Fatalf("new source spacing lost: %v %v", p.Header, q.Header)
	}
}

func TestRebaserTreatsHugeSequenceJumpAsSenderRestart(t *testing.T) {
	r, c := newRebaser()
	seqs, _ := feed(r, c, 7, [][2]uint32{{100, 1000}, {101, 1960}})
	p := pkt(7, 30000, 9999999)
	r.rewrite(p, 111)
	if p.SequenceNumber != seqs[1]+1 {
		t.Fatalf("restart must continue the output sequence: %d after %d", p.SequenceNumber, seqs[1])
	}
}

func TestRebaserRewritesPayloadTypeAndStripsExtensions(t *testing.T) {
	r, _ := newRebaser()
	p := pkt(7, 1, 1)
	p.Extension, p.ExtensionProfile = true, 0xbede
	r.rewrite(p, 111)
	if p.PayloadType != 111 || p.Extension || p.ExtensionProfile != 0 {
		t.Fatalf("header not normalised: %#v", p.Header)
	}
}

func TestRebaserKeepExtensionsLeavesThemAlone(t *testing.T) {
	r, _ := newRebaser()
	r.keepExtensions = true
	p := pkt(7, 1, 1)
	p.Extension, p.ExtensionProfile = true, 0xbede
	r.rewrite(p, p.PayloadType)
	if !p.Extension || p.ExtensionProfile != 0xbede {
		t.Fatalf("extensions must be kept for the Meta -> LiveKit direction: %#v", p.Header)
	}
}
