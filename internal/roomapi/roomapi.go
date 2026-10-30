// Package roomapi is the small slice of LiveKit's server API the connector
// needs to run a call without joining the room itself.
package roomapi

import (
	"context"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// API creates rooms, dispatches agents and observes participants.
type API interface {
	// EnsureRoom creates the room if it does not exist yet.
	EnsureRoom(ctx context.Context, room string) error
	CreateDispatch(ctx context.Context, room, agentName, metadata string) error
	ListParticipants(ctx context.Context, room string) ([]*livekit.ParticipantInfo, error)
	RemoveParticipant(ctx context.Context, room, identity string) error
}

type client struct {
	rooms    *lksdk.RoomServiceClient
	dispatch *lksdk.AgentDispatchClient
}

// New returns an API backed by the LiveKit server at httpURL.
func New(httpURL, apiKey, apiSecret string) API {
	return &client{
		rooms:    lksdk.NewRoomServiceClient(httpURL, apiKey, apiSecret),
		dispatch: lksdk.NewAgentDispatchServiceClient(httpURL, apiKey, apiSecret),
	}
}

func (c *client) EnsureRoom(ctx context.Context, room string) error {
	// CreateRoom is idempotent for an existing room. The short timeouts only
	// reclaim a room whose call never fully started.
	_, err := c.rooms.CreateRoom(ctx, &livekit.CreateRoomRequest{Name: room, EmptyTimeout: 60, DepartureTimeout: 20})
	return err
}

func (c *client) CreateDispatch(ctx context.Context, room, agentName, metadata string) error {
	_, err := c.dispatch.CreateDispatch(ctx, &livekit.CreateAgentDispatchRequest{Room: room, AgentName: agentName, Metadata: metadata})
	return err
}

func (c *client) ListParticipants(ctx context.Context, room string) ([]*livekit.ParticipantInfo, error) {
	res, err := c.rooms.ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: room})
	if err != nil {
		return nil, err
	}
	return res.Participants, nil
}

func (c *client) RemoveParticipant(ctx context.Context, room, identity string) error {
	_, err := c.rooms.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: room, Identity: identity})
	return err
}
