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
	CallID           string   `json:"call_id"`
	Action           string   `json:"action"`
	Session          *session `json:"session,omitempty"`
}
type session struct {
	SDPType string `json:"sdp_type"`
	SDP     string `json:"sdp"`
}

func (c *Client) PreAccept(ctx context.Context, version, phoneID, token, callID, sdp string) error {
	return c.call(ctx, version, phoneID, token, callRequest{"whatsapp", callID, "pre_accept", &session{"answer", sdp}})
}
func (c *Client) Accept(ctx context.Context, version, phoneID, token, callID, sdp string) error {
	return c.call(ctx, version, phoneID, token, callRequest{"whatsapp", callID, "accept", &session{"answer", sdp}})
}
func (c *Client) Terminate(ctx context.Context, version, phoneID, token, callID string) error {
	return c.call(ctx, version, phoneID, token, callRequest{"whatsapp", callID, "terminate", nil})
}

func (c *Client) call(ctx context.Context, version, phoneID, token string, body callRequest) error {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if version == "" {
		return fmt.Errorf("WhatsApp API version is required")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	u := strings.TrimRight(c.BaseURL, "/") + "/v" + version + "/" + phoneID + "/calls"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("Meta calls API returned HTTP %d: %s", resp.StatusCode, sanitize(string(b)))
}

func sanitize(s string) string {
	if len(s) > 512 {
		s = s[:512]
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
}
