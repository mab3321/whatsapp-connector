package livekitbridge

import (
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const (
	// audioLevelURI is the RFC 6464 header extension LiveKit forwards with every
	// audio packet. It lets the hub tell speech from silence without decoding.
	audioLevelURI = "urn:ietf:params:rtp-hdrext:ssrc-audio-level"
	// speechLevelMax is the quietest level (dBov, 0 is loudest, 127 is silence)
	// still counted as speech. Room tone and encoder noise sit well below it.
	speechLevelMax = 70
	// dtxMaxPayload is the largest payload an Opus DTX/silence frame has; used
	// when the track carries no audio level extension.
	dtxMaxPayload = 3
)

// levelReader extracts the audio level of a packet.
type levelReader struct{ id uint8 }

// levelReaderFor finds the negotiated audio level extension of a receiver.
func levelReaderFor(r *webrtc.RTPReceiver) levelReader {
	if r == nil {
		return levelReader{}
	}
	for _, ext := range r.GetParameters().HeaderExtensions {
		if ext.URI == audioLevelURI && ext.ID > 0 && ext.ID < 15 {
			return levelReader{id: uint8(ext.ID)}
		}
	}
	return levelReader{}
}

// speech reports whether the packet carries audible audio.
func (l levelReader) speech(p *rtp.Packet) bool {
	if l.id != 0 {
		if ext := p.GetExtension(l.id); len(ext) > 0 {
			return ext[0]&0x7f <= speechLevelMax
		}
	}
	return len(p.Payload) > dtxMaxPayload
}
