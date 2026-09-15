package connector

import (
	"context"
	"errors"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/mab3321/whatsapp-connector/internal/call"
	"github.com/twitchtv/twirp"
)

type Service struct{ calls *call.Manager }

func NewService(calls *call.Manager) *Service { return &Service{calls: calls} }

func (s *Service) AcceptWhatsAppCall(ctx context.Context, req *livekit.AcceptWhatsAppCallRequest) (*livekit.AcceptWhatsAppCallResponse, error) {
	if req == nil || req.Sdp == nil || req.Sdp.Type != "offer" {
		return nil, twirp.InvalidArgumentError("sdp", "an SDP offer is required")
	}
	timeout := 30 * time.Second
	if req.RingingTimeout != nil && req.RingingTimeout.AsDuration() > 0 {
		timeout = req.RingingTimeout.AsDuration()
	}
	room, err := s.calls.Accept(ctx, call.AcceptParams{
		PhoneID: req.WhatsappPhoneNumberId, APIToken: req.WhatsappApiKey,
		APIVersion: req.WhatsappCloudApiVersion, CallID: req.WhatsappCallId,
		SDP: req.Sdp.Sdp, RoomName: req.RoomName, Identity: req.ParticipantIdentity,
		Name: req.ParticipantName, Metadata: req.ParticipantMetadata,
		Attributes: req.ParticipantAttributes, Agents: req.Agents,
		Wait: req.WaitUntilAnswered, Timeout: timeout,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, twirp.NewError(twirp.DeadlineExceeded, "WhatsApp call setup timed out")
		}
		return nil, twirp.InvalidArgumentError("call", err.Error())
	}
	return &livekit.AcceptWhatsAppCallResponse{RoomName: room}, nil
}

func (s *Service) DisconnectWhatsAppCall(_ context.Context, req *livekit.DisconnectWhatsAppCallRequest) (*livekit.DisconnectWhatsAppCallResponse, error) {
	if req == nil || req.WhatsappCallId == "" {
		return nil, twirp.InvalidArgumentError("whatsapp_call_id", "is required")
	}
	user := req.DisconnectReason == livekit.DisconnectWhatsAppCallRequest_USER_INITIATED
	if !user && req.WhatsappApiKey == "" {
		return nil, twirp.InvalidArgumentError("whatsapp_api_key", "is required for business-initiated disconnect")
	}
	if err := s.calls.Disconnect(req.WhatsappCallId, user, req.WhatsappApiKey); err != nil {
		return nil, twirp.InternalErrorWith(err)
	}
	return &livekit.DisconnectWhatsAppCallResponse{}, nil
}

func (s *Service) DialWhatsAppCall(ctx context.Context, req *livekit.DialWhatsAppCallRequest) (*livekit.DialWhatsAppCallResponse, error) {
	if req == nil || req.WhatsappPhoneNumberId == "" || req.WhatsappToPhoneNumber == "" || req.WhatsappApiKey == "" || req.WhatsappCloudApiVersion == "" {
		return nil, twirp.InvalidArgumentError("call", "phone number id, destination, API key and API version are required")
	}
	timeout := 45 * time.Second
	if req.RingingTimeout != nil && req.RingingTimeout.AsDuration() > 0 {
		timeout = req.RingingTimeout.AsDuration()
	}
	callID, room, err := s.calls.Dial(ctx, call.DialParams{
		PhoneID: req.WhatsappPhoneNumberId, APIToken: req.WhatsappApiKey,
		APIVersion: req.WhatsappCloudApiVersion, To: req.WhatsappToPhoneNumber,
		Opaque: req.WhatsappBizOpaqueCallbackData, RoomName: req.RoomName,
		Identity: req.ParticipantIdentity, Name: req.ParticipantName,
		Metadata: req.ParticipantMetadata, Attributes: req.ParticipantAttributes,
		Agents: req.Agents, Timeout: timeout,
	})
	if err != nil {
		return nil, twirp.InvalidArgumentError("call", err.Error())
	}
	return &livekit.DialWhatsAppCallResponse{WhatsappCallId: callID, RoomName: room}, nil
}
func (s *Service) ConnectWhatsAppCall(ctx context.Context, req *livekit.ConnectWhatsAppCallRequest) (*livekit.ConnectWhatsAppCallResponse, error) {
	if req == nil || req.WhatsappCallId == "" || req.Sdp == nil || req.Sdp.Type != "answer" || req.Sdp.Sdp == "" {
		return nil, twirp.InvalidArgumentError("sdp", "a call id and SDP answer are required")
	}
	if err := s.calls.Connect(ctx, req.WhatsappCallId, req.Sdp.Sdp, req.WaitUntilAnswered); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, twirp.NewError(twirp.DeadlineExceeded, "WhatsApp call setup timed out")
		}
		return nil, twirp.InvalidArgumentError("call", err.Error())
	}
	return &livekit.ConnectWhatsAppCallResponse{}, nil
}
func (*Service) ConnectTwilioCall(context.Context, *livekit.ConnectTwilioCallRequest) (*livekit.ConnectTwilioCallResponse, error) {
	return nil, twirp.NewError(twirp.Unimplemented, "Twilio calling is not implemented")
}
