package whip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
)

// CreateServerOffer asks the server to create the offer, for calls where the
// remote only answers (outbound WhatsApp calls). As with Create, the server
// holds the response until the subscribed tracks exist, so the offer carries
// them. The remote's answer goes back with SendAnswer.
func (c *Client) CreateServerOffer(ctx context.Context, token string, subscribe map[string][]string) (offer, location string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(nil))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-LiveKit-Server-Offer", "1")
	if len(subscribe) > 0 {
		info, err := json.Marshal(map[string]any{"subscribedParticipantTrackNames": subscribe})
		if err != nil {
			return "", "", err
		}
		req.Header.Set("X-LiveKit-ClientInfo", string(info))
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated {
		return "", "", fmt.Errorf("LiveKit WHIP returned HTTP %d: %s", resp.StatusCode, oneLine(string(body)))
	}
	location = resp.Header.Get("Location")
	if location == "" || len(body) == 0 {
		return "", "", errors.New("LiveKit WHIP response had no offer or session location")
	}
	return string(body), location, nil
}

// SendAnswer gives the server the remote's answer to the offer from
// CreateServerOffer.
func (c *Client) SendAnswer(ctx context.Context, token, location, answer string) error {
	if !strings.HasPrefix(location, path+"/") {
		return fmt.Errorf("unexpected WHIP session location")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, strings.TrimRight(c.BaseURL, "/")+location, strings.NewReader(answer))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("LiveKit WHIP answer returned HTTP %d: %s", resp.StatusCode, oneLine(string(body)))
	}
	return nil
}

// NormalizeOffer prepares the server's offer for Meta: UDP candidates on
// publicIP only, the connection address and media port taken from the first of
// them, and the attributes that Meta's own known-good offers do not carry
// (per-stream SSRC and msid lines, mixed extmaps, reduced-size RTCP) removed.
func NormalizeOffer(offer string, publicIP netip.Addr) string {
	filtered := FilterAnswer(offer, publicIP)
	var lines []string
	for _, l := range strings.Split(filtered, "\n") {
		if t := strings.TrimRight(l, "\r"); t != "" {
			lines = append(lines, t)
		}
	}
	ip, port := "", ""
	for _, t := range lines {
		if strings.HasPrefix(t, "a=candidate:") {
			if f := strings.Fields(t); len(f) >= 6 {
				ip, port = f[4], f[5]
				break
			}
		}
	}
	out := make([]string, 0, len(lines))
	for _, t := range lines {
		switch {
		case strings.HasPrefix(t, "a=ssrc"), strings.HasPrefix(t, "a=msid:"),
			t == "a=extmap-allow-mixed", t == "a=rtcp-rsize":
			continue
		case ip != "" && strings.HasPrefix(t, "c=IN IP4 "):
			t = "c=IN IP4 " + ip
		case ip != "" && strings.HasPrefix(t, "m=audio "):
			if f := strings.Fields(t); len(f) >= 4 {
				f[1] = port
				t = strings.Join(f, " ")
			}
		}
		out = append(out, t)
	}
	return strings.Join(out, "\r\n") + "\r\n"
}
