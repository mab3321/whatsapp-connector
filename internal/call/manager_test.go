package call

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/mab3321/whatsapp-connector/internal/livekitbridge"
	"github.com/mab3321/whatsapp-connector/internal/media"
	"github.com/pion/rtp"
)

func testCall() *Call {
	ctx, cancel := context.WithCancel(context.Background())
	return &Call{log: slog.New(slog.NewTextHandler(io.Discard, nil)), ctx: ctx, cancel: cancel}
}

func await(t *testing.T, c *Call, bridge <-chan struct{}, media <-chan error, max time.Duration) string {
	t.Helper()
	got := make(chan string, 1)
	go func() { got <- c.awaitEnd(bridge, media, max) }()
	select {
	case r := <-got:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("awaitEnd did not return")
		return ""
	}
}

func TestAwaitEndMaxDurationTerminatesCall(t *testing.T) {
	c := testCall()
	reason := await(t, c, make(chan struct{}), make(chan error), 20*time.Millisecond)
	if reason != endMaxDuration {
		t.Fatalf("reason = %q, want %q", reason, endMaxDuration)
	}
	if !c.terminate || c.ctx.Err() == nil {
		t.Fatal("call must be cancelled and flagged for Meta termination")
	}
}

func TestAwaitEndZeroMaxDurationDisablesCap(t *testing.T) {
	c := testCall()
	done := make(chan string, 1)
	go func() { done <- c.awaitEnd(make(chan struct{}), make(chan error), 0) }()
	select {
	case r := <-done:
		t.Fatalf("returned %q with no end signal", r)
	case <-time.After(100 * time.Millisecond):
	}
	c.stopUser()
	if r := <-done; r != endUserHangup {
		t.Fatalf("reason = %q, want %q", r, endUserHangup)
	}
}

func TestAwaitEndReasons(t *testing.T) {
	t.Run("business disconnect", func(t *testing.T) {
		c := testCall()
		go c.stopBusiness()
		if r := await(t, c, make(chan struct{}), make(chan error), time.Hour); r != endDisconnected {
			t.Fatalf("reason = %q", r)
		}
	})
	t.Run("livekit closed", func(t *testing.T) {
		c := testCall()
		bridge := make(chan struct{})
		close(bridge)
		if r := await(t, c, bridge, make(chan error), time.Hour); r != endLiveKitClosed {
			t.Fatalf("reason = %q", r)
		}
		if !c.terminate {
			t.Fatal("Meta call must be terminated when LiveKit side closes")
		}
	})
	t.Run("media stopped", func(t *testing.T) {
		c := testCall()
		media := make(chan error, 1)
		media <- errors.New("inbound RTP stopped: i/o timeout")
		if r := await(t, c, make(chan struct{}), media, time.Hour); r != endMediaStopped {
			t.Fatalf("reason = %q", r)
		}
		if !c.terminate {
			t.Fatal("Meta call must be terminated when media stops")
		}
	})
}

func TestUserHangupDoesNotTerminateMeta(t *testing.T) {
	c := testCall()
	c.stopUser()
	if r := await(t, c, make(chan struct{}), make(chan error), time.Hour); r != endUserHangup {
		t.Fatalf("reason = %q", r)
	}
	if c.terminate {
		t.Fatal("Meta already ended the call; connector must not terminate it again")
	}
}

// Agent audio must never block while the Meta media path is not up: the
// bridge keeps reading its track and drops the packet.
func TestWriteToMetaNeverBlocksBeforeReady(t *testing.T) {
	c := testCall()
	c.transport = media.NewTransport(context.Background(), nil, nil, time.Second, time.Second)
	done := make(chan error, 1)
	go func() { done <- c.writeToMeta(&rtp.Packet{}) }()
	select {
	case err := <-done:
		if !errors.Is(err, livekitbridge.ErrNotReady) {
			t.Fatalf("err = %v, want ErrNotReady", err)
		}
	case <-time.After(time.Second):
		t.Fatal("writeToMeta blocked before the media path was ready")
	}
}

func TestWriteToMetaDropsOutboundAudioUntilAnswered(t *testing.T) {
	c := testCall()
	c.outbound = true
	c.answerReady = make(chan struct{})
	if err := c.writeToMeta(&rtp.Packet{}); !errors.Is(err, livekitbridge.ErrNotReady) {
		t.Fatalf("err = %v, want ErrNotReady before the callee answers", err)
	}
}
