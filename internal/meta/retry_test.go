package meta

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// server answers with the given status codes in order, then 200.
func server(t *testing.T, codes ...int) (*Client, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := int(calls.Add(1))
		if n <= len(codes) {
			w.WriteHeader(codes[n-1])
			_, _ = w.Write([]byte(`{"error":"x"}`))
			return
		}
		_, _ = w.Write([]byte(`{"calls":[{"id":"wacid.1"}]}`))
	}))
	t.Cleanup(srv.Close)
	c := New()
	c.BaseURL = srv.URL
	c.Backoff = func(int) time.Duration { return time.Millisecond }
	return c, &calls
}

func TestAcceptRetriesWhenAPIIsOverloaded(t *testing.T) {
	c, calls := server(t, http.StatusServiceUnavailable, http.StatusTooManyRequests)
	if err := c.Accept(context.Background(), "v23.0", "1", "tok", "call", "sdp"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
}

func TestPreAcceptDoesNotRetryServerErrorsThatMayHaveBeenProcessed(t *testing.T) {
	c, calls := server(t, http.StatusInternalServerError)
	if err := c.PreAccept(context.Background(), "v23.0", "1", "tok", "call", "sdp"); err == nil {
		t.Fatal("expected the 500 to be returned")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, a possibly-processed request must not be repeated", calls.Load())
	}
}

func TestClientErrorsAreNeverRetried(t *testing.T) {
	c, calls := server(t, http.StatusBadRequest)
	if err := c.Accept(context.Background(), "v23.0", "1", "tok", "call", "sdp"); err == nil {
		t.Fatal("expected the 400 to be returned")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestTerminateRetriesServerErrors(t *testing.T) {
	c, calls := server(t, http.StatusBadGateway, http.StatusInternalServerError)
	if err := c.Terminate(context.Background(), "v23.0", "1", "tok", "call"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
}

func TestTerminateGivesUpAfterMaxAttempts(t *testing.T) {
	c, calls := server(t, 500, 500, 500, 500, 500)
	if err := c.Terminate(context.Background(), "v23.0", "1", "tok", "call"); err == nil {
		t.Fatal("expected an error after exhausting retries")
	}
	if calls.Load() != maxAttempts {
		t.Fatalf("calls = %d, want %d", calls.Load(), maxAttempts)
	}
}

func TestDialIsNeverRetried(t *testing.T) {
	c, calls := server(t, http.StatusServiceUnavailable)
	if _, err := c.Dial(context.Background(), "v23.0", "1", "tok", "+15551234567", "", "sdp"); err == nil {
		t.Fatal("expected the error to be returned")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d: retrying an outbound call could ring the customer twice", calls.Load())
	}
}

func TestRetryStopsWhenContextEnds(t *testing.T) {
	c, calls := server(t, 503, 503, 503)
	c.Backoff = func(int) time.Duration { return time.Hour }
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Accept(ctx, "v23.0", "1", "tok", "call", "sdp"); err == nil {
		t.Fatal("expected an error")
	}
	if time.Since(start) > 2*time.Second || calls.Load() != 1 {
		t.Fatalf("took %s with %d calls", time.Since(start), calls.Load())
	}
}

func TestConnectionRefusedIsRetriedForAccept(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing listens any more: dial fails
	c := New()
	c.BaseURL = url
	var tries atomic.Int32
	c.Backoff = func(int) time.Duration { tries.Add(1); return time.Millisecond }
	if err := c.Accept(context.Background(), "v23.0", "1", "tok", "call", "sdp"); err == nil {
		t.Fatal("expected a connection error")
	}
	if tries.Load() != maxAttempts-1 {
		t.Fatalf("retried %d times, want %d", tries.Load(), maxAttempts-1)
	}
}
