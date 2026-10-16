package config

import (
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("PUBLIC_IP", "203.0.113.10")
	t.Setenv("LIVEKIT_API_KEY", "key")
	t.Setenv("LIVEKIT_API_SECRET", "secret")
}

func TestMaxCallDurationDefaultsToTwoHours(t *testing.T) {
	setRequired(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxCallDuration != 2*time.Hour {
		t.Fatalf("MaxCallDuration = %s, want 2h", c.MaxCallDuration)
	}
}

func TestMaxCallDurationFromEnv(t *testing.T) {
	setRequired(t)
	t.Setenv("MAX_CALL_DURATION", "45m")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxCallDuration != 45*time.Minute {
		t.Fatalf("MaxCallDuration = %s, want 45m", c.MaxCallDuration)
	}
}

func TestRTCPAndStatsIntervals(t *testing.T) {
	setRequired(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RTCPInterval != 2*time.Second || c.StatsInterval != 15*time.Second {
		t.Fatalf("defaults = %s / %s", c.RTCPInterval, c.StatsInterval)
	}
	t.Setenv("RTCP_INTERVAL", "off")
	t.Setenv("STATS_INTERVAL", "0")
	c, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RTCPInterval != 0 || c.StatsInterval != 0 {
		t.Fatalf("off/0 must disable both, got %s / %s", c.RTCPInterval, c.StatsInterval)
	}
	t.Setenv("RTCP_INTERVAL", "5s")
	if c, _ = Load(); c.RTCPInterval != 5*time.Second {
		t.Fatalf("RTCP_INTERVAL=5s gave %s", c.RTCPInterval)
	}
}
