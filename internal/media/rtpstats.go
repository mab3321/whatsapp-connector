package media

import (
	"math"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

const opusClockRate = 48000

// rxStats tracks reception quality of one inbound RTP source following
// RFC 3550 appendix A.1 (extended sequence numbers, loss, interarrival jitter)
// so the connector can send meaningful RTCP receiver reports to Meta.
type rxStats struct {
	mu sync.Mutex

	started  bool
	ssrc     uint32
	baseSeq  uint32
	maxSeq   uint16
	cycles   uint32
	received uint32
	reorder  uint64

	expectedPrior, receivedPrior uint32

	epoch       time.Time
	haveTransit bool
	transit     float64
	jitter      float64

	lastSR   uint32 // middle 32 bits of the last sender report NTP time
	lastSRAt time.Time
}

func (s *rxStats) update(p *rtp.Packet, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started || p.SSRC != s.ssrc {
		// New source: restart the counters but keep the last sender report so
		// round-trip time estimation survives an SSRC change. The mutex must
		// not be overwritten while held, so reset field by field.
		s.started, s.ssrc, s.baseSeq, s.maxSeq, s.cycles = true, p.SSRC, uint32(p.SequenceNumber), p.SequenceNumber, 0
		s.received, s.reorder, s.expectedPrior, s.receivedPrior = 0, 0, 0, 0
		s.epoch, s.haveTransit, s.transit, s.jitter = at, false, 0, 0
	} else if udelta := p.SequenceNumber - s.maxSeq; udelta < 0x8000 {
		if p.SequenceNumber < s.maxSeq {
			s.cycles += 1 << 16
		}
		s.maxSeq = p.SequenceNumber
	} else {
		s.reorder++
	}
	s.received++

	arrival := at.Sub(s.epoch).Seconds() * opusClockRate
	transit := arrival - float64(p.Timestamp)
	if s.haveTransit {
		d := math.Abs(transit - s.transit)
		s.jitter += (d - s.jitter) / 16
	}
	s.transit, s.haveTransit = transit, true
}

func (s *rxStats) noteSenderReport(ntp uint64, at time.Time) {
	s.mu.Lock()
	s.lastSR, s.lastSRAt = uint32(ntp>>16), at
	s.mu.Unlock()
}

// rxSnapshot is a point-in-time view used for logging.
type rxSnapshot struct {
	Received  uint32
	Lost      int64
	Reordered uint64
	JitterMs  float64
}

func (s *rxStats) snapshot() rxSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return rxSnapshot{}
	}
	expected := s.cycles + uint32(s.maxSeq) - s.baseSeq + 1
	return rxSnapshot{Received: s.received, Lost: int64(expected) - int64(s.received), Reordered: s.reorder, JitterMs: s.jitter / opusClockRate * 1000}
}

// report builds the reception report block for the current interval.
func (s *rxStats) report(now time.Time) (rtcp.ReceptionReport, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return rtcp.ReceptionReport{}, false
	}
	extMax := s.cycles + uint32(s.maxSeq)
	expected := extMax - s.baseSeq + 1
	lost := int64(expected) - int64(s.received)
	expInterval := expected - s.expectedPrior
	recInterval := s.received - s.receivedPrior
	s.expectedPrior, s.receivedPrior = expected, s.received
	var fraction uint8
	if lostInterval := int64(expInterval) - int64(recInterval); expInterval > 0 && lostInterval > 0 {
		fraction = uint8((lostInterval << 8) / int64(expInterval))
	}
	if lost < 0 {
		lost = 0 // duplicates can make the count negative; RTCP wants unsigned here
	}
	if lost > 0xffffff {
		lost = 0xffffff
	}
	var dlsr uint32
	if s.lastSR != 0 {
		dlsr = uint32(now.Sub(s.lastSRAt).Seconds() * 65536)
	}
	return rtcp.ReceptionReport{
		SSRC: s.ssrc, FractionLost: fraction, TotalLost: uint32(lost), LastSequenceNumber: extMax,
		Jitter: uint32(s.jitter), LastSenderReport: s.lastSR, Delay: dlsr,
	}, true
}

// txStats counts what the connector sends to Meta, for sender reports.
type txStats struct {
	mu      sync.Mutex
	ssrc    uint32
	packets uint32
	octets  uint32
	lastTS  uint32
	lastAt  time.Time
	hasSent bool
}

func (s *txStats) sent(p *rtp.Packet, at time.Time) {
	s.mu.Lock()
	s.ssrc, s.lastTS, s.lastAt, s.hasSent = p.SSRC, p.Timestamp, at, true
	s.packets++
	s.octets += uint32(len(p.Payload))
	s.mu.Unlock()
}

// ntpTime converts a wall clock time to a 64-bit NTP timestamp.
func ntpTime(t time.Time) uint64 {
	const ntpEpochOffset = 2208988800
	sec := uint64(t.Unix()) + ntpEpochOffset
	frac := uint64(t.Nanosecond()) * (1 << 32) / 1e9
	return sec<<32 | frac
}

// senderReport returns a sender report when audio has been sent.
func (s *txStats) senderReport(now time.Time) (*rtcp.SenderReport, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasSent {
		return nil, false
	}
	elapsed := now.Sub(s.lastAt).Seconds()
	return &rtcp.SenderReport{
		SSRC: s.ssrc, NTPTime: ntpTime(now), RTPTime: s.lastTS + uint32(elapsed*opusClockRate),
		PacketCount: s.packets, OctetCount: s.octets,
	}, true
}
