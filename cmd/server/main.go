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
	"github.com/fbufler/google-pubsub/internal/core/api/handler"
	pubsubinit "github.com/fbufler/google-pubsub/internal/init"
	storagemodels "github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/storage/uow"
	"github.com/fbufler/google-pubsub/internal/core/types"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8085"
	}

	// --- Storage setup ---
	initialState := &memory.State{
		Snapshots:       make(map[types.FQDN]*storagemodels.Snapshot),
		Topics:          make(map[types.FQDN]*storagemodels.Topic),
		Subscriptions:   make(map[types.FQDN]*storagemodels.Subscription),
		Messages:        make(map[types.FQDN]*storagemodels.Message),
		PendingMessages: make(map[types.FQDN][]*storagemodels.PendingMessage),
	}
	store := uow.NewMemoryStore(initialState)

	// --- Use cases ---
	topicUC := usecases.NewTopicUsecase(uow.NewMemoryUoW(store, func(s *memory.State) usecases.TopicProvider {
		return repositories.NewProvider(s)
	}))
	pubUC := usecases.NewPublisher(uow.NewMemoryUoW(store, func(s *memory.State) usecases.PublishProvider {
		return repositories.NewProvider(s)
	}))
	subUC := usecases.NewSubscriber(uow.NewMemoryUoW(store, func(s *memory.State) usecases.SubscriberProvider {
		return repositories.NewProvider(s)
	}))
	snapUC := usecases.NewSnapshotUsecase(uow.NewMemoryUoW(store, func(s *memory.State) usecases.SnapshotProvider {
		return repositories.NewProvider(s)
	}))

	// --- Optional init config: pre-create topics and subscriptions on startup ---
	if initPath := os.Getenv("INIT_CONFIG"); initPath != "" {
		cfg, err := pubsubinit.Load(initPath)
		if err != nil {
			slog.Error("failed to load init config", "path", initPath, "err", err)
			os.Exit(1)
		}
		if err := pubsubinit.Apply(cfg, topicUC, subUC); err != nil {
			slog.Error("failed to apply init config", "err", err)
			os.Exit(1)
		}
		slog.Info("init config applied", "path", initPath)
	}

	// --- Handlers ---
	pub := handler.NewPublisher(topicUC, pubUC)
	sub := handler.NewSubscriber(subUC, snapUC)

	mux := http.NewServeMux()

	// --- Health endpoints ---
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	mux.Handle(pubsubpbconnect.NewPublisherHandler(pub))
	mux.Handle(pubsubpbconnect.NewSubscriberHandler(sub))

	reflector := grpcreflect.NewStaticReflector(
		pubsubpbconnect.PublisherName,
		pubsubpbconnect.SubscriberName,
	)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	// --- gRPC-Gateway REST proxy ---
	ctx := context.Background()
	gwmux := runtime.NewServeMux()

	dialAddr := addr
	if len(dialAddr) > 0 && dialAddr[0] == ':' {
		dialAddr = "127.0.0.1" + dialAddr
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	if err := pubsubpb.RegisterPublisherHandlerFromEndpoint(ctx, gwmux, dialAddr, opts); err != nil {
		slog.Error("failed to register publisher gateway", "err", err)
		os.Exit(1)
	}
	if err := pubsubpb.RegisterSubscriberHandlerFromEndpoint(ctx, gwmux, dialAddr, opts); err != nil {
		slog.Error("failed to register subscriber gateway", "err", err)
		os.Exit(1)
	}

	mux.Handle("/", gwmux)

	slog.Info("starting pubsub emulator", "addr", addr)
	if err := http.ListenAndServe(addr, h2c.NewHandler(mux, &http2.Server{})); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
