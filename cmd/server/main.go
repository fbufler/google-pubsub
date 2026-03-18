package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"connectrpc.com/grpcreflect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pubsubpb "github.com/fbufler/google-pubsub/gen/google/pubsub/v1"
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

	// --- HEALTH ENDPOINTS ---
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	// ------------------------

	mux.Handle(pubsubpbconnect.NewPublisherHandler(pub))
	mux.Handle(pubsubpbconnect.NewSubscriberHandler(sub))

	reflector := grpcreflect.NewStaticReflector(
		pubsubpbconnect.PublisherName,
		pubsubpbconnect.SubscriberName,
	)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	// --- GRPC-GATEWAY REST PROXY SETUP ---
	ctx := context.Background()
	gwmux := runtime.NewServeMux()

	// The gRPC client needs a valid dial string (e.g., "localhost:8085" instead of ":8085")
	dialAddr := addr
	if len(dialAddr) > 0 && dialAddr[0] == ':' {
		dialAddr = "127.0.0.1" + dialAddr
	}

	// Tell the proxy to talk to our Connect server in HTTP/2 cleartext (h2c)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// Register both services to the gateway
	if err := pubsubpb.RegisterPublisherHandlerFromEndpoint(ctx, gwmux, dialAddr, opts); err != nil {
		slog.Error("failed to register publisher gateway", "err", err)
		os.Exit(1)
	}
	if err := pubsubpb.RegisterSubscriberHandlerFromEndpoint(ctx, gwmux, dialAddr, opts); err != nil {
		slog.Error("failed to register subscriber gateway", "err", err)
		os.Exit(1)
	}

	// Route all unmatched traffic (which includes REST paths like /v1/projects/...) through the gateway
	mux.Handle("/", gwmux)
	// -------------------------------------

	slog.Info("starting pubsub emulator", "addr", addr)
	// We use h2c so both gRPC (HTTP/2) and REST (HTTP/1.1) can share the exact same port
	if err := http.ListenAndServe(addr, h2c.NewHandler(mux, &http2.Server{})); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
