package livekitbridge

import (
	"github.com/pion/rtp"
	"testing"
)

func TestRTPTranslatorMaintainsContinuity(t *testing.T) {
	var tr rtpTranslator
	a := &rtp.Packet{Header: rtp.Header{PayloadType: 100, SSRC: 10, SequenceNumber: 8, Timestamp: 1000}}
	tr.rewrite(a, 111)
	b := &rtp.Packet{Header: rtp.Header{PayloadType: 100, SSRC: 99, SequenceNumber: 500, Timestamp: 2000}}
	tr.rewrite(b, 111)
	if b.PayloadType != 111 || b.SSRC != a.SSRC || b.SequenceNumber != a.SequenceNumber+1 || b.Timestamp != a.Timestamp+1000 {
		t.Fatalf("RTP continuity not preserved: first=%#v second=%#v", a.Header, b.Header)
	}
}
