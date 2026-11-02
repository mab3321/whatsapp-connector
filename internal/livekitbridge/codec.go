package livekitbridge

import "github.com/livekit/media-sdk"

// Codec supplies the Opus encoder and decoder used only while several tracks
// have to be mixed. A single live track never touches it: its Opus packets are
// forwarded as they are.
type Codec interface {
	// NewDecoder returns a decoder that writes 48 kHz mono PCM to out.
	NewDecoder(out media.PCM16Writer) (Decoder, error)
	// NewEncoder returns an encoder; every 20 ms Opus frame is handed to sink.
	NewEncoder(sink func(payload []byte) error) (media.PCM16Writer, error)
}

// Decoder turns one Opus RTP payload into PCM.
type Decoder interface {
	Decode(payload []byte) error
	Close() error
}
