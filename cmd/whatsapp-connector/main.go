package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/mab3321/whatsapp-connector/internal/call"
	"github.com/mab3321/whatsapp-connector/internal/config"
	connectorservice "github.com/mab3321/whatsapp-connector/internal/connector"
	"github.com/mab3321/whatsapp-connector/internal/meta"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	conf, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	manager := call.NewManager(log, call.Config{
		LiveKitURL: conf.LiveKitURL, APIKey: conf.LiveKitAPIKey, APISecret: conf.LiveKitSecret,
		PublicIP: conf.PublicIP, PortMin: conf.PortMin, PortMax: conf.PortMax,
		SetupTimeout: conf.SetupTimeout, MediaTimeout: conf.MediaTimeout,
	}, meta.New())
	service := connectorservice.NewService(manager)
	twirpHandler := livekit.NewConnectorServer(service)
	mux := http.NewServeMux()
	mux.Handle(livekit.ConnectorPathPrefix, connectorservice.AuthMiddleware(conf.LiveKitAPIKey, conf.LiveKitSecret, twirpHandler))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{Addr: conf.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("WhatsApp connector listening", "address", conf.HTTPAddr, "media_port_min", conf.PortMin, "media_port_max", conf.PortMax)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), conf.ShutdownWindow)
	defer cancel()
	_ = server.Shutdown(ctx)
	_ = manager.Shutdown(ctx)
}
