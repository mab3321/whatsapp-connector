package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
	// Backoff returns the pause before retry number n (1-based). Tests replace it.
	Backoff func(n int) time.Duration
}

// maxAttempts is the total number of tries for a retryable request.
const maxAttempts = 3

// retryPolicy says which failures may be retried for an action.
type retryPolicy int

const (
	// retryNever: the request may have side effects that a repeat would
	// duplicate (placing an outbound call).
	retryNever retryPolicy = iota
	// retryUnprocessed: retry only when the request was clearly not processed
	// (could not connect, or the API said it is overloaded). Used for
	// pre_accept and accept, where a repeat after a lost response could turn a
	// success into an error.
	retryUnprocessed
	// retryAny: terminate is safe to repeat, so retry timeouts and 5xx too.
	retryAny
)

type httpStatusError struct {
	code int
	msg  string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("Meta calls API returned HTTP %d: %s", e.code, e.msg)
}

func retryable(policy retryPolicy, err error) bool {
	if policy == retryNever || err == nil {
		return false
	}
	var se *httpStatusError
	if errors.As(err, &se) {
		if se.code == http.StatusTooManyRequests || se.code == http.StatusServiceUnavailable {
			return true
		}
		return policy == retryAny && se.code >= 500
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return true // never connected, so nothing was processed
	}
	return policy == retryAny // timeouts and resets after the request left
}

func New() *Client {
	return &Client{BaseURL: "https://graph.facebook.com", HTTP: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) backoff(n int) time.Duration {
	if c.Backoff != nil {
		return c.Backoff(n)
	}
	return time.Duration(n) * 400 * time.Millisecond
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
	_, err := c.call(ctx, retryUnprocessed, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", CallID: callID, Action: "pre_accept", Session: &session{"answer", sdp}})
	return err
}
func (c *Client) Accept(ctx context.Context, version, phoneID, token, callID, sdp string) error {
	_, err := c.call(ctx, retryUnprocessed, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", CallID: callID, Action: "accept", Session: &session{"answer", sdp}})
	return err
}
func (c *Client) Terminate(ctx context.Context, version, phoneID, token, callID string) error {
	_, err := c.call(ctx, retryAny, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", CallID: callID, Action: "terminate"})
	return err
}

func (c *Client) Dial(ctx context.Context, version, phoneID, token, to, opaque, sdp string) (string, error) {
	to = strings.TrimPrefix(strings.TrimSpace(to), "+")
	if to == "" {
		return "", fmt.Errorf("WhatsApp destination is required")
	}
	return c.call(ctx, retryNever, version, phoneID, token, callRequest{MessagingProduct: "whatsapp", To: to, Action: "connect", Session: &session{"offer", sdp}, Opaque: opaque})
}

// call performs the request, retrying transient failures according to policy.
func (c *Client) call(ctx context.Context, policy retryPolicy, version, phoneID, token string, body callRequest) (string, error) {
	var out string
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		out, err = c.callOnce(ctx, version, phoneID, token, body)
		if err == nil || attempt == maxAttempts || !retryable(policy, err) {
			return out, err
		}
		select {
		case <-time.After(c.backoff(attempt)):
		case <-ctx.Done():
			return "", err
		}
	}
	return out, err
}

func (c *Client) callOnce(ctx context.Context, version, phoneID, token string, body callRequest) (string, error) {
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
	return "", &httpStatusError{code: resp.StatusCode, msg: sanitize(string(b))}
}

func sanitize(s string) string {
	if len(s) > 512 {
		s = s[:512]
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
}
