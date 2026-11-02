package livekitbridge

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPresenceEndsTheCallOnlyAfterAudioWasPublishedAndEveryoneLeft(t *testing.T) {
	var ended atomic.Int32
	p := newPresence(30*time.Millisecond, func() { ended.Add(1) })
	defer p.stop()

	p.update(0) // nobody joined yet: the agent is still starting
	time.Sleep(80 * time.Millisecond)
	if ended.Load() != 0 {
		t.Fatal("ended before anybody published audio")
	}

	p.update(1)
	p.update(0)
	time.Sleep(80 * time.Millisecond)
	if ended.Load() != 1 {
		t.Fatalf("ended %d times, want once", ended.Load())
	}
}

func TestPresenceSurvivesAHandoverBetweenParticipants(t *testing.T) {
	var ended atomic.Int32
	p := newPresence(50*time.Millisecond, func() { ended.Add(1) })
	defer p.stop()

	p.update(1) // agent
	p.update(0) // agent left ...
	p.update(1) // ... but a human has joined within the grace period
	time.Sleep(120 * time.Millisecond)
	if ended.Load() != 0 {
		t.Fatal("the call ended during a handover")
	}
}
