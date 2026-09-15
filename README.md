# WhatsApp Connector

Self-hosted implementation of LiveKit's livekit.Connector Twirp service for
inbound and outbound WhatsApp Business Calling.

The service accepts the same AcceptWhatsAppCallRequest used by LiveKit SDKs,
negotiates Meta ICE/DTLS-SRTP, and relays Opus RTP directly to and from a
self-hosted LiveKit room. It does not transcode audio.

## Implemented RPCs

- AcceptWhatsAppCall
- DialWhatsAppCall
- ConnectWhatsAppCall
- DisconnectWhatsAppCall

Twilio RPCs return the standard Twirp unimplemented error.

For outbound calls, call `DialWhatsAppCall` first. When Meta sends the
`BUSINESS_INITIATED` connect webhook containing its SDP answer, forward it to
`ConnectWhatsAppCall`. The destination must have granted WhatsApp call
permission to the business; Meta rejects calls without that permission.

## Configuration

See .env.example. The media UDP range must be reachable from Meta. Never commit
LiveKit or Meta credentials.

## Build and test

    go test -race ./...
    docker build -t whatsapp-connector .

Transport code under internal/media is derived from the Apache-2.0 LiveKit SIP
implementation and retains its cancel-safe ICE/DTLS/SRTP teardown.
