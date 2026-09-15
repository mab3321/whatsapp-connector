package livekitbridge

import (
	"testing"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/rtp"
)

func TestEndsCallWhenAgentDeparts(t *testing.T) {
	if !endsCallOnDeparture(lksdk.ParticipantAgent) {
		t.Fatal("agent departure must end the WhatsApp call")
	}
	for _, kind := range []lksdk.ParticipantKind{
		lksdk.ParticipantStandard,
		lksdk.ParticipantSIP,
		lksdk.ParticipantConnector,
	} {
		if endsCallOnDeparture(kind) {
			t.Fatalf("participant kind %v must not end the WhatsApp call", kind)
		}
	}
}

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
