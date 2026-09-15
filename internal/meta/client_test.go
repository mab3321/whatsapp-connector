package meta

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCallActions(t *testing.T) {
	var actions []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v26.0/phone/calls" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing auth")
		}
		var b callRequest
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Error(err)
		}
		actions = append(actions, b.Action)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"success\":true}"))
	}))
	defer s.Close()
	c := New()
	c.BaseURL = s.URL
	ctx := context.Background()
	if err := c.PreAccept(ctx, "26.0", "phone", "secret", "call", "v=0"); err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(ctx, "v26.0", "phone", "secret", "call", "v=0"); err != nil {
		t.Fatal(err)
	}
	if err := c.Terminate(ctx, "26.0", "phone", "secret", "call"); err != nil {
		t.Fatal(err)
	}
	want := []string{"pre_accept", "accept", "terminate"}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("actions=%v", actions)
		}
	}
}

func TestDial(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b callRequest
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Fatal(err)
		}
		if b.Action != "connect" || b.To != "923094836196" || b.CallID != "" || b.Session == nil || b.Session.SDPType != "offer" || b.Opaque != "trace" {
			t.Fatalf("bad dial request: %#v", b)
		}
		_, _ = w.Write([]byte(`{"messaging_product":"whatsapp","calls":[{"id":"wacid.test"}]}`))
	}))
	defer s.Close()
	c := New()
	c.BaseURL = s.URL
	id, err := c.Dial(context.Background(), "26.0", "phone", "secret", "+923094836196", "trace", "v=0")
	if err != nil || id != "wacid.test" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}
