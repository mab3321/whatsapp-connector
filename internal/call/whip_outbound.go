package call

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	lkconnector "github.com/livekit/protocol/connector"
	"github.com/mab3321/whatsapp-connector/internal/whip"
)

// useDirectOutbound reports whether outbound calls let the LiveKit server
// terminate Meta's media. Like the inbound path it needs an agent to wait for.
func (m *Manager) useDirectOutbound(p DialParams) bool {
	return m.conf.DirectOutbound && m.rooms != nil && m.whip != nil &&
		(len(p.Agents) > 0 || m.conf.WhipAutoDispatch)
}

const (
	// DirectionAttribute tells the voice worker the call was placed by the connector:
	// the callee has not answered when the participant joins, so the agent must
	// not speak until the participant is active.
	DirectionAttribute = "call.media_direction"
	DirectionOutbound  = "outbound"
)

// outboundAttributes returns the participant attributes for a placed call.
func outboundAttributes(in map[string]string) map[string]string {
	out := directAttributes(in)
	out[DirectionAttribute] = DirectionOutbound
	return out
}

const (
	// AnsweredAttribute is set to "true" on a relay outbound call's participant
	// once the callee has answered and the media path is up. The voice worker
	// holds its greeting until then.
	AnsweredAttribute = "call.answered"
)

// relayOutboundAttributes returns the attributes of a relayed outbound call: the
// caller's own, marked as placed by the connector. Unlike a direct call it does
// not claim direct media.
func relayOutboundAttributes(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	out[DirectionAttribute] = DirectionOutbound
	return out
}

// dialWHIP places an outbound call whose media the LiveKit server terminates.
// The server creates the offer (Meta only answers), this connector carries it to
// Meta and brings the answer back; no RTP passes through the connector.
func (m *Manager) dialWHIP(ctx context.Context, p DialParams) (string, string, error) {
	if p.RoomName == "" {
		p.RoomName = "whatsapp-" + uuid.NewString()
	}
	if p.Identity == "" {
		p.Identity = "whatsapp-" + uuid.NewString()
	}
	setup := m.conf.SetupTimeout
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

	started := time.Now()
	setupCtx, cancelSetup := context.WithTimeout(ctx, agentWait+setup)
	defer cancelSetup()
	if err := m.rooms.EnsureRoom(setupCtx, p.RoomName); err != nil {
		directSetups.WithLabelValues("room").Inc()
		return "", "", fmt.Errorf("create room: %w", err)
	}
	for _, a := range p.Agents {
		if err := m.rooms.CreateDispatch(setupCtx, p.RoomName, a.AgentName, a.Metadata); err != nil {
			directSetups.WithLabelValues("dispatch").Inc()
			return "", "", fmt.Errorf("dispatch agent: %w", err)
		}
	}
	agentID, err := m.waitForAgent(setupCtx, p.RoomName, track, m.conf.AgentTrackWait != TrackWaitServer, poll, agentWait)
	if err != nil {
		directSetups.WithLabelValues("agent_not_ready").Inc()
		return "", "", err
	}
	directAgentWait.Observe(time.Since(started).Seconds())

	token, err := lkconnector.BuildConnectorToken(lkconnector.ConnectorTokenParams{
		APIKey: m.conf.APIKey, APISecret: m.conf.APISecret, RoomName: p.RoomName,
		ParticipantIdentity: p.Identity, ParticipantName: p.Name, ParticipantMetadata: p.Metadata,
		ParticipantAttributes: outboundAttributes(p.Attributes), Kind: lkconnector.WHATSAPP,
	})
	if err != nil {
		directSetups.WithLabelValues("token").Inc()
		return "", "", err
	}
	whipTimeout := m.conf.WhipTimeout
	if whipTimeout <= 0 {
		whipTimeout = defaultWhipTimeout
	}
	whipStarted := time.Now()
	whipCtx, cancelWHIP := context.WithTimeout(ctx, whipTimeout)
	offer, location, err := m.whip.CreateServerOffer(whipCtx, token, map[string][]string{agentID: {track}})
	cancelWHIP()
	if err != nil {
		directSetups.WithLabelValues("whip").Inc()
		return "", "", err
	}
	directWHIP.Observe(time.Since(whipStarted).Seconds())
	offer = whip.NormalizeOffer(offer, m.conf.PublicIP)

	callID, err := m.meta.Dial(ctx, p.APIVersion, p.PhoneID, p.APIToken, p.To, p.Opaque, offer)
	if err != nil {
		directSetups.WithLabelValues("meta_dial").Inc()
		m.dropWHIPSession(token, location, p.RoomName, p.Identity)
		return "", "", err
	}

	ap := AcceptParams{PhoneID: p.PhoneID, APIToken: p.APIToken, APIVersion: p.APIVersion, CallID: callID,
		RoomName: p.RoomName, Identity: p.Identity, Name: p.Name, Metadata: p.Metadata,
		Attributes: p.Attributes, Agents: p.Agents, Timeout: p.Timeout}
	callCtx, cancel := context.WithCancel(context.Background())
	c := &Call{log: m.log.With("call", callLabel(callID), "room", p.RoomName, "mode", ModeWHIP, "direction", "outbound"),
		params: ap, room: p.RoomName, identity: p.Identity, ctx: callCtx, cancel: cancel,
		outbound: true, direct: true, signaled: true, whipToken: token, whipLoc: location,
		ready: make(chan struct{}), answerReady: make(chan struct{}), done: make(chan struct{})}
	m.mu.Lock()
	if m.draining {
		m.mu.Unlock()
		c.stopBusiness()
		c.cleanup(m.meta)
		m.dropWHIPSession(token, location, p.RoomName, p.Identity)
		return "", "", errors.New("connector is draining")
	}
	m.calls[callID] = c
	m.wg.Add(1)
	m.mu.Unlock()
	go m.runOutboundWHIP(c)
	return callID, p.RoomName, nil
}

// dropWHIPSession ends a server session that never became a call.
func (m *Manager) dropWHIPSession(token, location, room, identity string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.whip.Delete(ctx, token, location); err != nil {
		_ = m.rooms.RemoveParticipant(ctx, room, identity)
	}
}

// runOutboundWHIP waits for Meta's answer, hands it to the server and watches the
// call. The answer arrives through Connect (Meta's webhook).
func (m *Manager) runOutboundWHIP(c *Call) {
	defer m.finish(c)
	setup := m.conf.SetupTimeout
	// How long the callee has to pick up.
	ring := c.params.Timeout
	if ring <= 0 {
		ring = setup
	}
	poll := m.conf.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	timer := time.NewTimer(ring)
	defer timer.Stop()
	select {
	case <-c.answerReady:
	case <-timer.C:
		c.failWHIP("no_answer", context.DeadlineExceeded)
		return
	case <-c.ctx.Done():
		return
	}
	c.mu.Lock()
	answer, token, loc := c.directAnswer, c.whipToken, c.whipLoc
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	err := m.whip.SendAnswer(ctx, token, loc, answer)
	cancel()
	if err != nil {
		c.failWHIP("whip_answer", err)
		return
	}
	if err := m.waitForMedia(c, poll, setup); err != nil {
		c.failWHIP("media", err)
		return
	}
	directSetups.WithLabelValues("connected").Inc()
	c.signalReady(nil)
	c.log.Info("WhatsApp call connected")
	connected := time.Now()
	reason := m.monitorWHIP(c, poll)
	c.log.Info("WhatsApp call ended", "reason", reason, "duration", time.Since(connected).Round(time.Second))
}
