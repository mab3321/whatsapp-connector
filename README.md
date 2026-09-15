# WhatsApp Connector

Self-hosted implementation of LiveKit's livekit.Connector Twirp service for
inbound WhatsApp Business Calling.

The service accepts the same AcceptWhatsAppCallRequest used by LiveKit SDKs,
negotiates Meta ICE/DTLS-SRTP, and relays Opus RTP directly to and from a
self-hosted LiveKit room. It does not transcode audio.

## Implemented RPCs

- AcceptWhatsAppCall
- DisconnectWhatsAppCall

Outbound WhatsApp and Twilio RPCs return the standard Twirp unimplemented
error so they can be added without changing the public service contract.

## Configuration

See .env.example. The media UDP range must be reachable from Meta. Never commit
LiveKit or Meta credentials.

## Build and test

    go test -race ./...
    docker build -t whatsapp-connector .

Transport code under internal/media is derived from the Apache-2.0 LiveKit SIP
implementation and retains its cancel-safe ICE/DTLS/SRTP teardown.
