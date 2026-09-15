package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New() *Client {
	return &Client{BaseURL: "https://graph.facebook.com", HTTP: &http.Client{Timeout: 10 * time.Second}}
}

type callRequest struct {
	MessagingProduct string   `json:"messaging_product"`
	CallID           string   `json:"call_id,omitempty"`
	To               string   `json:"to,omitempty"`
	Action           string   `json:"action"`
	Session          *session `json:"session,omitempty"`
	Opaque           string   `json:"biz_opaque_callback_data,omitempty"`
}
type session struct {
	SDPType string `json:"sdp_type"`
	SDP     string `json:"sdp"`
}

func (c *Client) PreAccept(ctx context.Context, version, phoneID, token, callID, sdp string) error {
	_, err := c.call(ctx, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", CallID: callID, Action: "pre_accept", Session: &session{"answer", sdp}})
	return err
}
func (c *Client) Accept(ctx context.Context, version, phoneID, token, callID, sdp string) error {
	_, err := c.call(ctx, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", CallID: callID, Action: "accept", Session: &session{"answer", sdp}})
	return err
}
func (c *Client) Terminate(ctx context.Context, version, phoneID, token, callID string) error {
	_, err := c.call(ctx, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", CallID: callID, Action: "terminate"})
	return err
}

func (c *Client) Dial(ctx context.Context, version, phoneID, token, to, opaque, sdp string) (string, error) {
	to = strings.TrimPrefix(strings.TrimSpace(to), "+")
	if to == "" {
		return "", fmt.Errorf("WhatsApp destination is required")
	}
	return c.call(ctx, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", To: to, Action: "connect", Session: &session{"offer", sdp}, Opaque: opaque})
}

func (c *Client) call(ctx context.Context, version, phoneID, token string, body callRequest) (string, error) {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if version == "" {
		return "", fmt.Errorf("WhatsApp API version is required")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	u := strings.TrimRight(c.BaseURL, "/") + "/v" + version + "/" + phoneID + "/calls"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var out struct {
			Calls []struct {
				ID string `json:"id"`
			} `json:"calls"`
		}
		if body.Action == "connect" && body.CallID == "" {
			if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil || len(out.Calls) == 0 || out.Calls[0].ID == "" {
				return "", fmt.Errorf("Meta calls API returned no call id")
			}
			return out.Calls[0].ID, nil
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return "", fmt.Errorf("Meta calls API returned HTTP %d: %s", resp.StatusCode, sanitize(string(b)))
}

func sanitize(s string) string {
	if len(s) > 512 {
		s = s[:512]
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
}
