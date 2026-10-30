# LiveKit server patch for direct WhatsApp media

`livekit-whip-direct.patch` is a unified diff against the LiveKit v1.13.7 source
(the `v1.13.7-sip-cluster` fork tree). It changes `pkg/service/roommanager_service.go`
and adds `pkg/service/whip_wait_test.go`:

- the WHIP create holds the offer for up to 10s until the requested agent tracks are
  published, stopping on request cancellation or participant closure;
- the subscription wait is bounded by the same deadline (it used to loop forever);
- a participant whose create fails is removed from the room.

Apply and build (from the root of the source tree):

    patch -p1 --binary < livekit-whip-direct.patch
    docker build -t livekit-server:v1.13.7-sip-cluster-direct .

Also set `psrpc: {max_attempts: 1, timeout: 30s}` in `livekit.yaml`; without it the
internal RPC retries a held request and replaces the participant.
