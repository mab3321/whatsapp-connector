package livekitbridge

import (
	"sync"
	"time"

	"github.com/pion/rtp"
)

const (
	opusFrameTicks = 960 // 20 ms at 48 kHz
	// A sequence jump larger than this means the sender restarted, not that
	// packets were lost or reordered.
	maxSeqJump = 3000
	// Cap on how many frames of wall-clock silence a rebase may insert.
	maxRebaseFrames = 500
)

// rtpRebaser rewrites SSRC and payload type while keeping the sender's own
// sequence numbers and timestamps, shifted by a constant offset. Because the
// offset is constant, packet loss shows up as a sequence gap, reordering stays
// reordering, and duplicates stay duplicates, so the receiver can conceal,
// reorder and discard them correctly. Renumbering every packet (the previous
// behaviour) hid loss and turned each reordered or duplicated packet into a
// permanent timeline shift.
//
// The offset only changes when the source changes (new SSRC or a sequence
// restart); the output then continues from the last position instead of
// jumping, using the elapsed wall-clock time to place it on the timeline.
type rtpRebaser struct {
	mu  sync.Mutex
	now func() time.Time

	started bool
	outSSRC uint32
	srcSSRC uint32
	seqOff  uint16
	tsOff   uint32

	lastInSeq  uint16 // highest input sequence of the current source
	lastOutSeq uint16 // highest output sequence emitted
	lastOutTS  uint32 // timestamp that went with lastOutSeq
	lastAt     time.Time

	// keepExtensions leaves RTP header extensions untouched (used for the
	// Meta -> LiveKit direction, where they were never modified).
	keepExtensions bool
}

func (r *rtpRebaser) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func seqNewer(a, b uint16) bool { return int16(a-b) > 0 }

func absInt16(v int16) int {
	if v < 0 {
		return -int(v)
	}
	return int(v)
}

func (r *rtpRebaser) rewrite(p *rtp.Packet, pt uint8) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	if !r.started {
		r.started = true
		r.outSSRC = p.SSRC ^ 0x6d2b79f5
		r.srcSSRC = p.SSRC
		r.lastInSeq = p.SequenceNumber
		r.lastOutSeq, r.lastOutTS = p.SequenceNumber, p.Timestamp
	} else if p.SSRC != r.srcSSRC || absInt16(int16(p.SequenceNumber-r.lastInSeq)) > maxSeqJump {
		frames := int(now.Sub(r.lastAt) / (20 * time.Millisecond))
		if frames < 1 {
			frames = 1
		}
		if frames > maxRebaseFrames {
			frames = maxRebaseFrames
		}
		nextSeq := r.lastOutSeq + 1
		nextTS := r.lastOutTS + uint32(frames)*opusFrameTicks
		r.seqOff = nextSeq - p.SequenceNumber
		r.tsOff = nextTS - p.Timestamp
		r.srcSSRC = p.SSRC
		r.lastInSeq = p.SequenceNumber
		r.lastOutSeq, r.lastOutTS = nextSeq-1, nextTS-opusFrameTicks
	}
	r.lastAt = now

	outSeq := p.SequenceNumber + r.seqOff
	outTS := p.Timestamp + r.tsOff
	if seqNewer(p.SequenceNumber, r.lastInSeq) {
		r.lastInSeq = p.SequenceNumber
	}
	if seqNewer(outSeq, r.lastOutSeq) {
		r.lastOutSeq, r.lastOutTS = outSeq, outTS
	}
	p.PayloadType, p.SSRC, p.SequenceNumber, p.Timestamp = pt, r.outSSRC, outSeq, outTS
	if r.keepExtensions {
		return
	}
	// The LiveKit WebRTC hop and Meta transport negotiate extensions
	// independently. Forwarding LiveKit's extension IDs to Meta violates the
	// Meta SDP, so emit a plain RTP header on this hop.
	p.Extension = false
	p.ExtensionProfile = 0
	p.Extensions = nil
}
