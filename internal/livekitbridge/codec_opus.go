//go:build cgo

package livekitbridge

import (
	"github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/opus"
	"github.com/livekit/protocol/logger"
)

// OpusCodec is the libopus based Codec (the same one LiveKit's SIP service uses).
// It is only available in cgo builds.
func OpusCodec() Codec { return opusCodec{} }

type opusCodec struct{}

func (opusCodec) NewDecoder(out media.PCM16Writer) (Decoder, error) {
	w, err := opus.Decode(out, 1, logger.GetLogger())
	if err != nil {
		return nil, err
	}
	return opusDecoder{w}, nil
}

func (opusCodec) NewEncoder(sink func(payload []byte) error) (media.PCM16Writer, error) {
	return opus.Encode(frameSink{sink}, 1, logger.GetLogger())
}

type opusDecoder struct{ w opus.Writer }

func (d opusDecoder) Decode(payload []byte) error { return d.w.WriteSample(opus.Sample(payload)) }
func (d opusDecoder) Close() error                { return d.w.Close() }

// frameSink adapts a plain callback to the writer the Opus encoder emits into.
type frameSink struct{ fn func([]byte) error }

func (frameSink) String() string  { return "opus frames -> RTP" }
func (frameSink) SampleRate() int { return 48000 }
func (frameSink) Close() error    { return nil }
func (s frameSink) WriteSample(f opus.Sample) error {
	return s.fn(f)
}
