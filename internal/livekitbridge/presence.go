package livekitbridge

import (
	"sync"
	"time"
)

const defaultEmptyGrace = 2 * time.Second

// presence ends a call once everyone who was publishing audio has left. Unlike
// "the agent left", it lets a call carry on when a human takes over from the
// agent, and it ignores listeners that never publish (monitoring, egress).
type presence struct {
	mu      sync.Mutex
	grace   time.Duration
	onEmpty func()
	seen    bool // somebody has published audio at least once
	timer   *time.Timer
	done    bool
}

func newPresence(grace time.Duration, onEmpty func()) *presence {
	if grace <= 0 {
		grace = defaultEmptyGrace
	}
	return &presence{grace: grace, onEmpty: onEmpty}
}

// update reports how many remote participants currently publish audio.
func (p *presence) update(audible int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	if audible > 0 {
		p.seen = true
		if p.timer != nil {
			p.timer.Stop()
			p.timer = nil
		}
		return
	}
	if !p.seen || p.timer != nil {
		return
	}
	p.timer = time.AfterFunc(p.grace, func() {
		p.mu.Lock()
		fire := !p.done && p.timer != nil
		p.done = true
		p.mu.Unlock()
		if fire {
			p.onEmpty()
		}
	})
}

func (p *presence) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done = true
	if p.timer != nil {
		p.timer.Stop()
	}
}
