// Package whip is a minimal client for LiveKit's built-in WHIP endpoint. In
// LiveKit's one-shot signalling mode a single SDP offer yields a single SDP
// answer, which is exactly the exchange the WhatsApp Calling API offers, so the
// LiveKit server can terminate Meta's media directly.
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

const path = "/whip/v1"

// Client talks to a LiveKit server's WHIP endpoint.
type Client struct {
	BaseURL string // http(s)://host:port
	HTTP    *http.Client
}

// Create posts an SDP offer. The server holds the response until every track in
// subscribe (publisher identity -> track names) is subscribed, because a track
// that appears after the answer cannot be added without renegotiation, which
// Meta does not support. Bound the wait with ctx.
func (c *Client) Create(ctx context.Context, token, offer string, subscribe map[string][]string) (answer, location string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewBufferString(offer))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", "Bearer "+token)
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
		return "", "", errors.New("LiveKit WHIP response had no answer or session location")
	}
	return string(body), location, nil
}

// Delete ends a WHIP session. It is safe to call for a session that is gone.
func (c *Client) Delete(ctx context.Context, token, location string) error {
	if !strings.HasPrefix(location, path+"/") {
		return fmt.Errorf("unexpected WHIP session location")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(c.BaseURL, "/")+location, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("LiveKit WHIP delete returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func oneLine(s string) string {
	if len(s) > 256 {
		s = s[:256]
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
}

// FilterAnswer keeps only the UDP candidates on publicIP. The LiveKit answer
// also lists TCP and container-internal addresses that Meta cannot use; sending
// them only adds noise to Meta's ICE checks. If filtering would remove every
// candidate the answer is returned unchanged.
func FilterAnswer(answer string, publicIP netip.Addr) string {
	lines := strings.Split(answer, "\n")
	out := make([]string, 0, len(lines))
	kept := 0
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\r")
		if strings.HasPrefix(trimmed, "a=candidate:") {
			f := strings.Fields(trimmed)
			// a=candidate:<foundation> <component> <proto> <priority> <ip> <port> typ ...
			if len(f) < 8 || !strings.EqualFold(f[2], "udp") || f[4] != publicIP.String() {
				continue
			}
			kept++
		}
		out = append(out, line)
	}
	if kept == 0 {
		return answer
	}
	return strings.Join(out, "\n")
}
