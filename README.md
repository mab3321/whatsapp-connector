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

Direct media is enabled with `WHATSAPP_DIRECT_MEDIA_ENABLED=true` (same as
`INBOUND_MODE=whip`). `WHATSAPP_DIRECT_MEDIA_PERCENT` (1-100, default 100) sends only
that share of inbound calls down this path while the rest stay on relay; the choice
is made once per call, and rolling back means setting the flag to false. A direct
call that fails setup is rejected, it does not fall back to relay.

Requirements and limits:

- The connector adds the participant attribute `call.media_mode=direct`. The voice
  worker must react to it by publishing its audio track
  (`AGENT_AUDIO_TRACK_NAME`, default `roomio_audio`) as soon as it sees the caller
  **join**, before it loads configuration. It must not wait for the caller to be
  ACTIVE: a WHIP participant only becomes ACTIVE after the SDP answer, and the
  answer waits for the agent's track. WHIP is a single offer/answer, so a track
  published afterwards is never attached.
- The LiveKit server must hold the offer until the agent's track exists
  (`AGENT_TRACK_WAIT=server`, needs the patched server in `deploy/`), or the
  connector waits for the track itself (`AGENT_TRACK_WAIT=connector`, stock server).
- The WHIP request has a deadline (`WHIP_REQUEST_TIMEOUT`, default 12s). LiveKit's
  internal RPC retries a slow request and each retry replaces the participant
  (`DUPLICATE_IDENTITY`), so set `psrpc: {max_attempts: 1, timeout: 30s}` in
  `livekit.yaml`.
- Prometheus metrics: `whatsapp_direct_setup_total{result}`,
  `whatsapp_direct_agent_ready_seconds`, `whatsapp_direct_whip_seconds`,
  `whatsapp_direct_answer_seconds`, `whatsapp_direct_cleanup_total{result}`. Packet
  and loss counters for direct calls come from the LiveKit server.

`go test -tags e2e ./e2e` runs a full call against a real LiveKit server with a
Meta stub (see the file header for the environment it needs).
