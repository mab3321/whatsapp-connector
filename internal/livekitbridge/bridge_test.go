package livekitbridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	lksdk "github.com/livekit/server-sdk-go/v2"
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

func TestWriteErrorPolicy(t *testing.T) {
	for _, err := range []error{io.EOF, io.ErrClosedPipe, net.ErrClosed, context.Canceled, fmt.Errorf("wrapped: %w", io.EOF)} {
		if !isFatalWrite(err) {
			t.Fatalf("%v must stop the forwarder", err)
		}
	}
	for _, err := range []error{ErrNotReady, errors.New("sendto: no buffer space"), fmt.Errorf("x: %w", ErrNotReady)} {
		if isFatalWrite(err) {
			t.Fatalf("%v must not stop the forwarder", err)
		}
	}
}
