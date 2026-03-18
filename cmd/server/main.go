package main

import (
	"log/slog"
	"net/http"
	"os"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/handler"
	pubsubinit "github.com/fbufler/google-pubsub/internal/init"
	"github.com/fbufler/google-pubsub/internal/storage"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8085"
	}

	store := storage.New()

	// Optional init config: pre-create topics and subscriptions on startup.
	if initPath := os.Getenv("INIT_CONFIG"); initPath != "" {
		cfg, err := pubsubinit.Load(initPath)
		if err != nil {
			slog.Error("failed to load init config", "path", initPath, "err", err)
			os.Exit(1)
		}
		if err := pubsubinit.Apply(cfg, store); err != nil {
			slog.Error("failed to apply init config", "err", err)
			os.Exit(1)
		}
		slog.Info("init config applied", "path", initPath)
	}

	pub := handler.NewPublisher(store)
	sub := handler.NewSubscriber(store)

	mux := http.NewServeMux()
	mux.Handle(pubsubpbconnect.NewPublisherHandler(pub))
	mux.Handle(pubsubpbconnect.NewSubscriberHandler(sub))

	slog.Info("starting pubsub emulator", "addr", addr)
	if err := http.ListenAndServe(addr, h2c.NewHandler(mux, &http2.Server{})); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
