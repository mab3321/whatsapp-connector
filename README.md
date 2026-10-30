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

The Meta media leg currently uses IPv4 UDP ICE candidates only. Configure an
IPv4 `PUBLIC_IP` and permit the configured UDP port range through the firewall.
IPv6 and TCP ICE candidates are ignored. The connector accepts Opus audio; it
does not transcode other codecs.

`MEDIA_TIMEOUT` is the maximum time to wait for an inbound RTP packet after
media is established (default: 20 seconds). If no packet arrives in that time,
the call ends and the connector logs an inbound RTP timeout. Increase this value
if long silent periods are expected. Incoming RTCP reception reports are logged
with loss and jitter information for diagnosing media quality.

The connector API supports the initial WhatsApp SDP offer and answer. It has no
active-call renegotiation method. A new SDP/ICE generation during a call cannot
be applied through the current LiveKit Connector API.

## Build and test

    go test -race ./...
    docker build -t whatsapp-connector .

Transport code under internal/media is derived from the Apache-2.0 LiveKit SIP
implementation and retains its cancel-safe ICE/DTLS/SRTP teardown.

## Inbound media modes (`INBOUND_MODE`)

`relay` (default) terminates Meta's ICE/DTLS-SRTP in the connector and relays
RTP into the room, as described above. Outbound calls always use this path.

`whip` hands Meta's SDP offer to the LiveKit server's built-in WHIP endpoint
(`POST /whip/v1`), so LiveKit terminates the media itself and the connector only
does signalling: it creates the room, dispatches the agent, posts the offer, sends
Meta `pre_accept`/`accept`, and watches the room for hang-up. Inbound calls with
no agent dispatch fall back to `relay`.

Requirements and limits, all verified against a LiveKit 1.13.7 server:

- The agent's audio track (`AGENT_AUDIO_TRACK_NAME`, default `roomio_audio`) must
  already be published when the offer is posted. WHIP is a single offer/answer, so
  a track published afterwards is never attached to the call (even forcing it with
  `UpdateSubscriptions` delivers no audio). A worker that publishes its audio only
  after `wait_for_participant()` therefore cannot be served in this mode; the
  connector fails the call with a clear error after `AGENT_WAIT_TIMEOUT`.
- Do not rely on the server holding the request for a late track: LiveKit's
  internal RPC retries the held request every few seconds, and each retry replaces
  the participant (`DUPLICATE_IDENTITY`), which looks like a hang-up to the agent.

`go test -tags e2e ./e2e` runs a full call against a real LiveKit server with a
Meta stub (see the file header for the environment it needs).
