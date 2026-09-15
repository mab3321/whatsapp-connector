package config

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr       string
	LiveKitURL     string
	LiveKitAPIKey  string
	LiveKitSecret  string
	PublicIP       netip.Addr
	PortMin        int
	PortMax        int
	SetupTimeout   time.Duration
	MediaTimeout   time.Duration
	ShutdownWindow time.Duration
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:       env("HTTP_ADDR", "127.0.0.1:8090"),
		LiveKitURL:     env("LIVEKIT_URL", "ws://127.0.0.1:7880"),
		LiveKitAPIKey:  os.Getenv("LIVEKIT_API_KEY"),
		LiveKitSecret:  os.Getenv("LIVEKIT_API_SECRET"),
		PortMin:        envInt("MEDIA_PORT_MIN", 40000),
		PortMax:        envInt("MEDIA_PORT_MAX", 49999),
		SetupTimeout:   envDuration("SETUP_TIMEOUT", 30*time.Second),
		MediaTimeout:   envDuration("MEDIA_TIMEOUT", 20*time.Second),
		ShutdownWindow: envDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
	var err error
	c.PublicIP, err = netip.ParseAddr(os.Getenv("PUBLIC_IP"))
	if err != nil || !c.PublicIP.Is4() {
		return Config{}, errors.New("PUBLIC_IP must be a valid IPv4 address")
	}
	if c.LiveKitAPIKey == "" || c.LiveKitSecret == "" {
		return Config{}, errors.New("LIVEKIT_API_KEY and LIVEKIT_API_SECRET are required")
	}
	if c.PortMin < 1024 || c.PortMax > 65535 || c.PortMin > c.PortMax {
		return Config{}, fmt.Errorf("invalid media port range %d-%d", c.PortMin, c.PortMax)
	}
	return c, nil
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err == nil && v != 0 {
		return v
	}
	return d
}
func envDuration(k string, d time.Duration) time.Duration {
	v, err := time.ParseDuration(os.Getenv(k))
	if err == nil && v > 0 {
		return v
	}
	return d
}
