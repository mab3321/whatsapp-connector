//go:build !cgo

package livekitbridge

// OpusCodec is nil without cgo; the hub then forwards a single track and never
// mixes (an overlap keeps the first speaker).
func OpusCodec() Codec { return nil }
