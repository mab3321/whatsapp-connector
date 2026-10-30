# LiveKit server patch for direct WhatsApp media

`livekit-whip-direct.patch` is a unified diff against the LiveKit v1.13.7 source
(the `v1.13.7-sip-cluster` fork tree). It changes `pkg/service/roommanager_service.go`
and `pkg/rtc/{transport,transportmanager,participant}.go`, and adds
`pkg/service/whip_wait_test.go`:

- the WHIP create holds the offer for up to 10s until the requested agent tracks are
  published, stopping on request cancellation or participant closure;
- the subscription wait is bounded by the same deadline (it used to loop forever);
- a participant whose create fails is removed from the room;
- WHIP participants of kind CONNECTOR answer as DTLS client (`a=setup:active`). Meta's
  media servers are ICE-lite and offer `actpass`, and pion would otherwise answer
  `passive` and wait for a ClientHello that never comes (ICE connects, then the peer
  connection fails about a second later). Adds `pkg/rtc/transport_dtls_test.go`.

Apply and build (from the root of the source tree):

    patch -p1 --binary < livekit-whip-direct.patch
    docker build -t livekit-server:v1.13.7-sip-cluster-direct .

Also set `psrpc: {max_attempts: 1, timeout: 30s}` in `livekit.yaml`; without it the
internal RPC retries a held request and replaces the participant.
