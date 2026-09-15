// Derived from github.com/livekit/sip (Apache-2.0), DTLS-SRTP/ICE work by mab3321.
package media

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"strconv"
	"strings"
	"time"

	pice "github.com/pion/ice/v4"
	psdp "github.com/pion/sdp/v3"
)

var ErrInvalidSDP = errors.New("invalid Meta DTLS-SRTP SDP")

type Certificate struct {
	certificate tls.Certificate
	Fingerprint string
}
type ICECandidate struct {
	Raw        string
	Address    netip.AddrPort
	Foundation string
	Priority   uint32
	Type       pice.CandidateType
}
type Negotiation struct {
	RemoteFingerprint                            string
	RemoteSetup, LocalSetup                      string
	IsClient                                     bool
	Certificate                                  *Certificate
	RemoteUfrag, RemotePwd, LocalUfrag, LocalPwd string
	RemoteCandidates                             []ICECandidate
	Local                                        netip.AddrPort
	PayloadType                                  uint8
	Answer                                       string
}

func NewCertificate() (*Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	t := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "whatsapp-connector"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	raw, err := x509.CreateCertificate(rand.Reader, t, t, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	d := sha256.Sum256(raw)
	parts := make([]string, len(d))
	for i, b := range d {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return &Certificate{certificate: tls.Certificate{Certificate: [][]byte{raw}, PrivateKey: key}, Fingerprint: strings.Join(parts, ":")}, nil
}

func attr(m *psdp.MediaDescription, key string) (string, bool) {
	for _, a := range m.Attributes {
		if a.Key == key {
			return a.Value, true
		}
	}
	return "", false
}
func sattr(s *psdp.SessionDescription, key string) (string, bool) {
	for _, a := range s.Attributes {
		if a.Key == key {
			return a.Value, true
		}
	}
	return "", false
}

func ParseOfferAndAnswer(raw string, cert *Certificate, publicIP netip.Addr, port int) (*Negotiation, error) {
	var offer psdp.SessionDescription
	if err := offer.Unmarshal([]byte(raw)); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSDP, err)
	}
	for _, m := range offer.MediaDescriptions {
		if m.MediaName.Media != "audio" || !strings.EqualFold(strings.Join(m.MediaName.Protos, "/"), "UDP/TLS/RTP/SAVPF") {
			continue
		}
		fp, ok := attr(m, "fingerprint")
		if !ok {
			fp, ok = sattr(&offer, "fingerprint")
		}
		if !ok {
			return nil, fmt.Errorf("%w: fingerprint missing", ErrInvalidSDP)
		}
		f := strings.Fields(fp)
		if len(f) != 2 || !strings.EqualFold(f[0], "sha-256") {
			return nil, fmt.Errorf("%w: SHA-256 fingerprint required", ErrInvalidSDP)
		}
		decoded, e := hex.DecodeString(strings.ReplaceAll(f[1], ":", ""))
		if e != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf("%w: malformed fingerprint", ErrInvalidSDP)
		}
		setup, ok := attr(m, "setup")
		if !ok {
			setup, ok = sattr(&offer, "setup")
		}
		if !ok {
			return nil, fmt.Errorf("%w: setup missing", ErrInvalidSDP)
		}
		if _, ok = attr(m, "rtcp-mux"); !ok {
			return nil, fmt.Errorf("%w: rtcp-mux required", ErrInvalidSDP)
		}
		ufrag, ok := attr(m, "ice-ufrag")
		if !ok {
			ufrag, ok = sattr(&offer, "ice-ufrag")
		}
		if !ok || ufrag == "" {
			return nil, fmt.Errorf("%w: ICE ufrag missing", ErrInvalidSDP)
		}
		pwd, ok := attr(m, "ice-pwd")
		if !ok {
			pwd, ok = sattr(&offer, "ice-pwd")
		}
		if !ok || pwd == "" {
			return nil, fmt.Errorf("%w: ICE password missing", ErrInvalidSDP)
		}
		pt, err := opusPayload(m)
		if err != nil {
			return nil, err
		}
		cands, err := remoteCandidates(m)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidSDP, err)
		}
		lu, err := credential(8)
		if err != nil {
			return nil, err
		}
		lp, err := credential(24)
		if err != nil {
			return nil, err
		}
		n := &Negotiation{RemoteFingerprint: strings.ToUpper(f[1]), RemoteSetup: strings.ToLower(setup), Certificate: cert, RemoteUfrag: ufrag, RemotePwd: pwd, LocalUfrag: lu, LocalPwd: lp, RemoteCandidates: cands, Local: netip.AddrPortFrom(publicIP, uint16(port)), PayloadType: pt}
		switch n.RemoteSetup {
		case "actpass", "active":
			n.LocalSetup = "passive"
			n.IsClient = false
		case "passive":
			n.LocalSetup = "active"
			n.IsClient = true
		default:
			return nil, fmt.Errorf("%w: unsupported setup role", ErrInvalidSDP)
		}
		n.Answer = buildAnswer(n)
		return n, nil
	}
	return nil, fmt.Errorf("%w: compatible audio media missing", ErrInvalidSDP)
}

func opusPayload(m *psdp.MediaDescription) (uint8, error) {
	for _, a := range m.Attributes {
		if a.Key != "rtpmap" {
			continue
		}
		f := strings.Fields(a.Value)
		if len(f) != 2 || !strings.EqualFold(f[1], "opus/48000/2") {
			continue
		}
		v, e := strconv.Atoi(f[0])
		if e == nil && v >= 0 && v <= 127 {
			return uint8(v), nil
		}
	}
	return 0, fmt.Errorf("%w: opus/48000/2 missing", ErrInvalidSDP)
}
func remoteCandidates(m *psdp.MediaDescription) ([]ICECandidate, error) {
	var out []ICECandidate
	seen := map[string]struct{}{}
	for _, a := range m.Attributes {
		if a.Key != "candidate" {
			continue
		}
		c, e := pice.UnmarshalCandidate(a.Value)
		if e != nil || c.Component() != pice.ComponentRTP || c.NetworkType() != pice.NetworkTypeUDP4 {
			continue
		}
		ip, e := netip.ParseAddr(c.Address())
		if e != nil || !ip.Is4() || c.Port() < 1 || c.Port() > 65535 {
			continue
		}
		raw := c.Marshal()
		if _, ok := seen[raw]; ok {
			continue
		}
		seen[raw] = struct{}{}
		out = append(out, ICECandidate{Raw: raw, Address: netip.AddrPortFrom(ip, uint16(c.Port())), Foundation: c.Foundation(), Priority: c.Priority(), Type: c.Type()})
	}
	if len(out) == 0 {
		return nil, errors.New("supported component-1 UDP candidate missing")
	}
	return out, nil
}
func credential(n int) (string, error) {
	b := make([]byte, n)
	if _, e := rand.Read(b); e != nil {
		return "", e
	}
	return hex.EncodeToString(b), nil
}
func buildAnswer(n *Negotiation) string {
	sid := uint64(time.Now().UnixNano())
	ip := n.Local.Addr().String()
	pt := strconv.Itoa(int(n.PayloadType))
	return fmt.Sprintf("v=0\r\no=- %d 2 IN IP4 %s\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\na=msid-semantic: WMS\r\nm=audio %d UDP/TLS/RTP/SAVPF %s\r\nc=IN IP4 %s\r\na=mid:0\r\na=sendrecv\r\na=rtcp-mux\r\na=rtpmap:%s opus/48000/2\r\na=fmtp:%s minptime=10;useinbandfec=1\r\na=ice-ufrag:%s\r\na=ice-pwd:%s\r\na=candidate:1 1 udp 2130706431 %s %d typ host\r\na=end-of-candidates\r\na=fingerprint:sha-256 %s\r\na=setup:%s\r\n", sid, ip, n.Local.Port(), pt, ip, pt, pt, n.LocalUfrag, n.LocalPwd, ip, n.Local.Port(), n.Certificate.Fingerprint, n.LocalSetup)
}
