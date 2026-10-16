package media

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

func rp(seq uint16, ts uint32) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{SSRC: 5, SequenceNumber: seq, Timestamp: ts}}
}

func TestRxStatsCountsLossAndFraction(t *testing.T) {
	var s rxStats
	at := time.Unix(1000, 0)
	for i, seq := range []uint16{1, 2, 3, 6, 7, 8, 9, 10} { // 4 and 5 lost
		s.update(rp(seq, uint32(i)*960), at.Add(time.Duration(i)*20*time.Millisecond))
	}
	rr, ok := s.report(at.Add(time.Second))
	if !ok {
		t.Fatal("expected a report")
	}
	if rr.TotalLost != 2 || rr.LastSequenceNumber != 10 || rr.SSRC != 5 {
		t.Fatalf("unexpected report: %+v", rr)
	}
	// 2 lost of 10 expected => 51/256, about 20 percent
	if rr.FractionLost < 50 || rr.FractionLost > 52 {
		t.Fatalf("fraction lost = %d, want about 51", rr.FractionLost)
	}
	// Next interval with no further loss reports zero.
	s.update(rp(11, 8*960), at.Add(2*time.Second))
	if rr2, _ := s.report(at.Add(3 * time.Second)); rr2.FractionLost != 0 {
		t.Fatalf("fraction lost after a clean interval = %d", rr2.FractionLost)
	}
}

func TestRxStatsHandlesWrapAndReordering(t *testing.T) {
	var s rxStats
	at := time.Unix(1000, 0)
	for i, seq := range []uint16{65534, 65535, 1, 0, 2} { // 0 arrives after 1
		s.update(rp(seq, uint32(i)*960), at.Add(time.Duration(i)*20*time.Millisecond))
	}
	snap := s.snapshot()
	if snap.Reordered != 1 {
		t.Fatalf("reordered = %d, want 1", snap.Reordered)
	}
	if snap.Lost != 0 {
		t.Fatalf("a reordered packet is not lost: lost = %d", snap.Lost)
	}
	rr, _ := s.report(at.Add(time.Second))
	if rr.LastSequenceNumber != 1<<16+2 {
		t.Fatalf("extended highest sequence = %d, want %d", rr.LastSequenceNumber, 1<<16+2)
	}
}

func TestRxStatsJitterGrowsWithIrregularArrival(t *testing.T) {
	var steady, bursty rxStats
	at := time.Unix(1000, 0)
	for i := 0; i < 50; i++ {
		steady.update(rp(uint16(i), uint32(i)*960), at.Add(time.Duration(i)*20*time.Millisecond))
		jit := time.Duration(0)
		if i%2 == 0 {
			jit = 15 * time.Millisecond
		}
		bursty.update(rp(uint16(i), uint32(i)*960), at.Add(time.Duration(i)*20*time.Millisecond+jit))
	}
	if steady.snapshot().JitterMs > 0.5 {
		t.Fatalf("steady stream jitter = %.2f ms", steady.snapshot().JitterMs)
	}
	if bursty.snapshot().JitterMs < 3 {
		t.Fatalf("irregular stream jitter = %.2f ms, want clearly above steady", bursty.snapshot().JitterMs)
	}
}

func TestRxStatsEchoesSenderReportForRTT(t *testing.T) {
	var s rxStats
	at := time.Unix(1000, 0)
	s.update(rp(1, 0), at)
	ntp := ntpTime(at)
	s.noteSenderReport(ntp, at)
	rr, _ := s.report(at.Add(500 * time.Millisecond))
	if rr.LastSenderReport != uint32(ntp>>16) {
		t.Fatalf("LSR = %d, want %d", rr.LastSenderReport, uint32(ntp>>16))
	}
	if rr.Delay < 32000 || rr.Delay > 33500 { // 0.5 s in 1/65536 s units
		t.Fatalf("DLSR = %d, want about 32768", rr.Delay)
	}
}

func TestTxStatsBuildsSenderReport(t *testing.T) {
	var s txStats
	now := time.Unix(2000, 0)
	if _, ok := s.senderReport(now); ok {
		t.Fatal("no sender report before anything was sent")
	}
	p := &rtp.Packet{Header: rtp.Header{SSRC: 9, Timestamp: 48000}, Payload: make([]byte, 100)}
	s.sent(p, now)
	s.sent(p, now)
	sr, ok := s.senderReport(now.Add(time.Second))
	if !ok || sr.SSRC != 9 || sr.PacketCount != 2 || sr.OctetCount != 200 {
		t.Fatalf("unexpected SR: %+v", sr)
	}
	if sr.RTPTime != 48000+48000 {
		t.Fatalf("RTP time should advance with the wall clock, got %d", sr.RTPTime)
	}
}
