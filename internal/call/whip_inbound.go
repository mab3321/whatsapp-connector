package call

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	lkconnector "github.com/livekit/protocol/connector"
	"github.com/livekit/protocol/livekit"
	"github.com/mab3321/whatsapp-connector/internal/roomapi"
	"github.com/mab3321/whatsapp-connector/internal/whip"
)

// TrackWaitServer makes the connector post the offer as soon as the agent has
// joined, leaving the wait for its audio track to a patched LiveKit server.
const TrackWaitServer = "server"

// Inbound media modes.
const (
	// ModeRelay terminates Meta's media in the connector and relays RTP into the
	// room as a participant (the original design, still used for outbound).
	ModeRelay = "relay"
	// ModeWHIP hands Meta's SDP to the LiveKit server, which terminates the media
	// itself; the connector only does signalling and watches the call.
	ModeWHIP = "whip"
)

// whipClient is the part of whip.Client the manager uses.
type whipClient interface {
	Create(ctx context.Context, token, offer string, subscribe map[string][]string) (answer, location string, err error)
	Delete(ctx context.Context, token, location string) error
}

var _ whipClient = (*whip.Client)(nil)

const (
	defaultAgentAudioTrack = "roomio_audio"
	defaultAgentWait       = 20 * time.Second
	defaultPollInterval    = time.Second
	// maxMonitorErrors is how many consecutive failed room lookups are tolerated
	// before the call is treated as lost.
	maxMonitorErrors = 5
)

func (m *Manager) useWHIP(p AcceptParams) bool {
	// Without any agent dispatch there is no agent to wait for, and the answer
	// would be sent before any audio track exists. Automatic dispatch (a worker
	// without an agent name) is opt-in because the connector cannot see it.
	return m.conf.InboundMode == ModeWHIP && m.rooms != nil && m.whip != nil && (len(p.Agents) > 0 || m.conf.WhipAutoDispatch)
}

func (m *Manager) acceptWHIP(ctx context.Context, p AcceptParams) (string, error) {
	if p.RoomName == "" {
		p.RoomName = "whatsapp-" + uuid.NewString()
	}
	if p.Identity == "" {
		p.Identity = "whatsapp-" + uuid.NewString()
	}
	callCtx, cancel := context.WithCancel(context.Background())
	c := &Call{
		log:    m.log.With("call", callLabel(p.CallID), "room", p.RoomName, "mode", ModeWHIP),
		params: p, room: p.RoomName, identity: p.Identity, ctx: callCtx, cancel: cancel,
		ready: make(chan struct{}), done: make(chan struct{}),
	}
	m.mu.Lock()
	if old := m.calls[p.CallID]; old != nil {
		m.mu.Unlock()
		cancel()
		if p.Wait {
			return old.room, old.wait(ctx)
		}
		return old.room, nil
	}
	m.calls[p.CallID] = c
	m.wg.Add(1)
	m.mu.Unlock()
	go m.runWHIP(c)
	if p.Wait {
		err := c.wait(ctx)
		if err != nil {
			c.stopBusiness()
		}
		return c.room, err
	}
	return c.room, nil
}

func (m *Manager) runWHIP(c *Call) {
	defer m.finish(c)
	p := c.params
	setup := m.conf.SetupTimeout
	if p.Timeout > 0 && p.Timeout < setup {
		setup = p.Timeout
	}
	agentWait := m.conf.AgentWaitTimeout
	if agentWait <= 0 {
		agentWait = defaultAgentWait
	}
	poll := m.conf.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	track := m.conf.AgentAudioTrack
	if track == "" {
		track = defaultAgentAudioTrack
	}

	// 1. The room and its agent must exist before the caller joins, and the
	// agent's identity must be known so the server can wait for its audio.
	setupCtx, cancelSetup := context.WithTimeout(c.ctx, agentWait+setup)
	defer cancelSetup()
	if err := m.rooms.EnsureRoom(setupCtx, c.room); err != nil {
		c.fail(fmt.Errorf("create room: %w", err))
		return
	}
	for _, a := range p.Agents {
		if err := m.rooms.CreateDispatch(setupCtx, c.room, a.AgentName, a.Metadata); err != nil {
			c.fail(fmt.Errorf("dispatch agent: %w", err))
			return
		}
	}
	agentID, err := m.waitForAgent(setupCtx, c.room, track, m.conf.AgentTrackWait != TrackWaitServer, poll, agentWait)
	if err != nil {
		c.fail(err)
		return
	}

	// 2. One offer in, one answer out. The server holds the request until the
	// agent's audio track is subscribed, so media is complete when it returns.
	token, err := lkconnector.BuildConnectorToken(lkconnector.ConnectorTokenParams{
		APIKey: m.conf.APIKey, APISecret: m.conf.APISecret, RoomName: c.room,
		ParticipantIdentity: c.identity, ParticipantName: p.Name, ParticipantMetadata: p.Metadata,
		ParticipantAttributes: p.Attributes, Kind: lkconnector.WHATSAPP,
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.mu.Lock()
	c.whipToken = token
	c.mu.Unlock()
	whipCtx, cancelWHIP := context.WithTimeout(c.ctx, agentWait+setup)
	answer, location, err := m.whip.Create(whipCtx, token, p.SDP, map[string][]string{agentID: {track}})
	cancelWHIP()
	if err != nil {
		c.fail(err)
		return
	}
	c.mu.Lock()
	c.whipLoc = location
	c.mu.Unlock()
	answer = whip.FilterAnswer(answer, m.conf.PublicIP)

	// 3. Same Meta sequence as the relay path: pre_accept starts media, accept
	// follows once the media path is up.
	if err = m.meta.PreAccept(c.ctx, p.APIVersion, p.PhoneID, p.APIToken, p.CallID, answer); err != nil {
		c.fail(err)
		return
	}
	c.mu.Lock()
	c.signaled = true
	c.mu.Unlock()
	if err = m.waitForMedia(c, poll, setup); err != nil {
		c.fail(err)
		return
	}
	if err = m.meta.Accept(c.ctx, p.APIVersion, p.PhoneID, p.APIToken, p.CallID, answer); err != nil {
		c.fail(err)
		return
	}
	c.signalReady(nil)
	c.log.Info("WhatsApp call connected")
	started := time.Now()
	reason := m.monitorWHIP(c, poll)
	c.log.Info("WhatsApp call ended", "reason", reason, "duration", time.Since(started).Round(time.Second))
}

// waitForAgent returns the identity of an agent whose audio track is already
// published. The WHIP server can only send tracks that exist when the offer
// arrives (a track published later is never attached and the held request
// times out), so the offer must not be posted before then.
func (m *Manager) waitForAgent(ctx context.Context, room, track string, needTrack bool, poll, limit time.Duration) (string, error) {
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	tick := time.NewTicker(minDuration(poll, 250*time.Millisecond))
	defer tick.Stop()
	joined := false
	for {
		parts, err := m.rooms.ListParticipants(ctx, room)
		if err == nil {
			for _, pi := range parts {
				if pi.Kind != livekit.ParticipantInfo_AGENT {
					continue
				}
				joined = true
				if !needTrack {
					return pi.Identity, nil
				}
				for _, t := range pi.Tracks {
					if t.Type == livekit.TrackType_AUDIO && t.Name == track {
						return pi.Identity, nil
					}
				}
			}
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			if joined {
				return "", fmt.Errorf("agent joined but did not publish audio track %q before the caller; the worker must publish its audio first in whip mode", track)
			}
			return "", errors.New("agent did not join the room in time")
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// waitForMedia blocks until the server reports the caller's connection active.
func (m *Manager) waitForMedia(c *Call, poll, limit time.Duration) error {
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	tick := time.NewTicker(minDuration(poll, 100*time.Millisecond))
	defer tick.Stop()
	for {
		if parts, err := m.rooms.ListParticipants(c.ctx, c.room); err == nil {
			for _, pi := range parts {
				if pi.Identity == c.identity && pi.State == livekit.ParticipantInfo_ACTIVE {
					return nil
				}
			}
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			return errors.New("WhatsApp media did not connect in time")
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
}

// monitorWHIP watches a connected call and returns why it ended. LiveKit owns
// the media, so the signals are room membership and the duration cap.
func (m *Manager) monitorWHIP(c *Call, poll time.Duration) string {
	var capCh <-chan time.Time
	if d := m.conf.MaxCallDuration; d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		capCh = t.C
	}
	tick := time.NewTicker(poll)
	defer tick.Stop()
	failures := 0
	for {
		select {
		case <-c.ctx.Done():
			c.mu.Lock()
			user := c.userTerminated
			c.mu.Unlock()
			if user {
				return endUserHangup
			}
			return endDisconnected
		case <-capCh:
			c.log.Warn("call exceeded maximum duration; terminating", "max_duration", m.conf.MaxCallDuration)
			c.stopBusiness()
			return endMaxDuration
		case <-tick.C:
			parts, err := m.rooms.ListParticipants(c.ctx, c.room)
			if err != nil {
				if failures++; failures >= maxMonitorErrors {
					c.log.Warn("room lookups kept failing", "error", err)
					c.stopBusiness()
					return endMediaStopped
				}
				continue
			}
			failures = 0
			caller, agent := false, false
			for _, pi := range parts {
				switch {
				case pi.Identity == c.identity:
					caller = pi.State != livekit.ParticipantInfo_DISCONNECTED
				case pi.Kind == livekit.ParticipantInfo_AGENT:
					agent = true
				}
			}
			if !caller {
				c.stopBusiness()
				return endMediaStopped
			}
			if !agent {
				c.stopBusiness()
				return endLiveKitClosed
			}
		}
	}
}

// cleanupWHIP ends the server-side session. The participant leaving is what
// tells the agent the call is over.
func (m *Manager) cleanupWHIP(c *Call) {
	c.mu.Lock()
	token, loc := c.whipToken, c.whipLoc
	c.mu.Unlock()
	if m.whip == nil || m.rooms == nil || token == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if loc != "" {
		if err := m.whip.Delete(ctx, token, loc); err == nil {
			return
		} else {
			c.log.Warn("WHIP session delete failed", "error", err)
		}
	}
	if err := m.rooms.RemoveParticipant(ctx, c.room, c.identity); err != nil && !strings.Contains(err.Error(), "not_found") {
		c.log.Warn("could not remove WhatsApp participant", "error", err)
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// NewRoomAPI builds the LiveKit API client from the connector's WebSocket URL.
func NewRoomAPI(conf Config) roomapi.API {
	return roomapi.New(httpURL(conf.LiveKitURL), conf.APIKey, conf.APISecret)
}

// NewWHIPClient builds the WHIP client from the connector's WebSocket URL.
func NewWHIPClient(conf Config) *whip.Client {
	return &whip.Client{BaseURL: httpURL(conf.LiveKitURL)}
}

func httpURL(u string) string {
	switch {
	case strings.HasPrefix(u, "wss://"):
		return "https://" + strings.TrimPrefix(u, "wss://")
	case strings.HasPrefix(u, "ws://"):
		return "http://" + strings.TrimPrefix(u, "ws://")
	}
	return u
}
