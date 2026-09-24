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

// NewOffer creates the local half of a business-initiated WhatsApp call. The
// returned negotiation is completed by ApplyAnswer before starting Transport.
func NewOffer(cert *Certificate, publicIP netip.Addr, port int) (*Negotiation, error) {
	if cert == nil || !publicIP.Is4() || port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w: invalid local transport", ErrInvalidSDP)
	}
	lu, err := credential(8)
	if err != nil {
		return nil, err
	}
	lp, err := credential(24)
	if err != nil {
		return nil, err
	}
	n := &Negotiation{
		Certificate: cert, LocalUfrag: lu, LocalPwd: lp,
		Local: netip.AddrPortFrom(publicIP, uint16(port)), PayloadType: 111,
		LocalSetup: "actpass",
	}
	n.Answer = buildOffer(n)
	return n, nil
}

// ApplyAnswer validates Meta's answer and adds its transport parameters to an
// outbound negotiation without replacing the credentials advertised by Offer.
func ApplyAnswer(raw string, n *Negotiation) error {
	if n == nil || n.Certificate == nil || n.LocalUfrag == "" || n.LocalPwd == "" {
		return fmt.Errorf("%w: outbound offer state missing", ErrInvalidSDP)
	}
	m, session, err := compatibleAudio(raw)
	if err != nil {
		return err
	}
	fp, ok := attr(m, "fingerprint")
	if !ok {
		fp, ok = sattr(session, "fingerprint")
	}
	if !ok {
		return fmt.Errorf("%w: fingerprint missing", ErrInvalidSDP)
	}
	f := strings.Fields(fp)
	if len(f) != 2 || !strings.EqualFold(f[0], "sha-256") {
		return fmt.Errorf("%w: SHA-256 fingerprint required", ErrInvalidSDP)
	}
	decoded, e := hex.DecodeString(strings.ReplaceAll(f[1], ":", ""))
	if e != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("%w: malformed fingerprint", ErrInvalidSDP)
	}
	setup, ok := attr(m, "setup")
	if !ok {
		setup, ok = sattr(session, "setup")
	}
	if !ok {
		return fmt.Errorf("%w: setup missing", ErrInvalidSDP)
	}
	if _, ok = attr(m, "rtcp-mux"); !ok {
		return fmt.Errorf("%w: rtcp-mux required", ErrInvalidSDP)
	}
	ufrag, ok := attr(m, "ice-ufrag")
	if !ok {
		ufrag, ok = sattr(session, "ice-ufrag")
	}
	pwd, pok := attr(m, "ice-pwd")
	if !pok {
		pwd, pok = sattr(session, "ice-pwd")
	}
	if !ok || !pok || ufrag == "" || pwd == "" {
		return fmt.Errorf("%w: ICE credentials missing", ErrInvalidSDP)
	}
	pt, err := opusPayload(m)
	if err != nil {
		return err
	}
	cands, err := remoteCandidates(m)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSDP, err)
	}
	n.RemoteFingerprint, n.RemoteSetup = strings.ToUpper(f[1]), strings.ToLower(setup)
	n.RemoteUfrag, n.RemotePwd, n.RemoteCandidates, n.PayloadType = ufrag, pwd, cands, pt
	switch n.RemoteSetup {
	case "active":
		n.LocalSetup, n.IsClient = "passive", false
	case "passive":
		n.LocalSetup, n.IsClient = "active", true
	default:
		return fmt.Errorf("%w: answer setup must be active or passive", ErrInvalidSDP)
	}
	return nil
}

func compatibleAudio(raw string) (*psdp.MediaDescription, *psdp.SessionDescription, error) {
	var session psdp.SessionDescription
	if err := session.Unmarshal([]byte(raw)); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidSDP, err)
	}
	for _, m := range session.MediaDescriptions {
		if m.MediaName.Media == "audio" && strings.EqualFold(strings.Join(m.MediaName.Protos, "/"), "UDP/TLS/RTP/SAVPF") {
			return m, &session, nil
		}
	}
	return nil, nil, fmt.Errorf("%w: compatible audio media missing", ErrInvalidSDP)
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
		if e == nil && v >= 0 && v <= 127 && offeredFormat(m, f[0]) {
			return uint8(v), nil
		}
	}
	return 0, fmt.Errorf("%w: opus/48000/2 missing", ErrInvalidSDP)
}
func offeredFormat(m *psdp.MediaDescription, format string) bool {
	for _, offered := range m.MediaName.Formats {
		if offered == format {
			return true
		}
	}
	return false
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

func buildOffer(n *Negotiation) string {
	sid := uint64(time.Now().UnixNano())
	ip := n.Local.Addr().String()
	return fmt.Sprintf("v=0\r\no=- %d 2 IN IP4 %s\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\na=msid-semantic: WMS\r\nm=audio %d UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 %s\r\na=mid:0\r\na=sendrecv\r\na=rtcp-mux\r\na=rtpmap:111 opus/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\na=ice-ufrag:%s\r\na=ice-pwd:%s\r\na=candidate:1 1 udp 2130706431 %s %d typ host\r\na=end-of-candidates\r\na=fingerprint:sha-256 %s\r\na=setup:actpass\r\n", sid, ip, n.Local.Port(), ip, n.LocalUfrag, n.LocalPwd, ip, n.Local.Port(), n.Certificate.Fingerprint)
}
