//go:build benchmark

package benchmark_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func emulatorHost() string {
	if h := os.Getenv("PUBSUB_EMULATOR_HOST"); h != "" {
		return h
	}
	return "localhost:8085"
}

func projectID() string {
	if p := os.Getenv("PUBSUB_PROJECT_ID"); p != "" {
		return p
	}
	return "test-project"
}

func newBenchClient(b *testing.B) *pubsub.Client {
	b.Helper()
	ctx := context.Background()
	conn, err := grpc.NewClient(emulatorHost(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("grpc.NewClient: %v", err)
	}
	b.Cleanup(func() { conn.Close() })
	client, err := pubsub.NewClient(ctx, projectID(),
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		b.Fatalf("pubsub.NewClient: %v", err)
	}
	b.Cleanup(func() { client.Close() })
	return client
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// BenchmarkPublishSingle measures the latency of publishing one message at a time.
// DelayThreshold and CountThreshold are both set to 1 to force an immediate flush
// on every call, so the measurement reflects server round-trip time, not batching delay.
func BenchmarkPublishSingle(b *testing.B) {
	client := newBenchClient(b)
	ctx := context.Background()

	topic, err := client.CreateTopic(ctx, uniqueName("bench-single"))
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	b.Cleanup(func() { _ = topic.Delete(context.Background()) })
	topic.PublishSettings.DelayThreshold = 1 * time.Millisecond
	topic.PublishSettings.CountThreshold = 1

	payload := []byte("benchmark-single")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := topic.Publish(ctx, &pubsub.Message{Data: payload}).Get(ctx); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// BenchmarkPublishBatch100 measures throughput when publishing 100 messages per iteration.
// The client batches automatically; b.N counts the number of 100-message batches.
func BenchmarkPublishBatch100(b *testing.B) {
	client := newBenchClient(b)
	ctx := context.Background()

	topic, err := client.CreateTopic(ctx, uniqueName("bench-batch"))
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	b.Cleanup(func() { _ = topic.Delete(context.Background()) })

	const batchSize = 100
	payload := []byte("benchmark-batch")

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(batchSize))
	for i := 0; i < b.N; i++ {
		results := make([]*pubsub.PublishResult, batchSize)
		for j := 0; j < batchSize; j++ {
			results[j] = topic.Publish(ctx, &pubsub.Message{Data: payload})
		}
		for j, r := range results {
			if _, err := r.Get(ctx); err != nil {
				b.Fatalf("batch[%d]: %v", j, err)
			}
		}
	}
}

// BenchmarkEndToEnd measures the full publish-to-pull round-trip for a single message.
// A persistent Receive goroutine is kept alive across iterations to avoid streaming
// pull setup overhead being counted per-iteration.
func BenchmarkEndToEnd(b *testing.B) {
	client := newBenchClient(b)
	ctx := context.Background()

	topic, err := client.CreateTopic(ctx, uniqueName("bench-e2e-topic"))
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	b.Cleanup(func() { _ = topic.Delete(context.Background()) })
	topic.PublishSettings.DelayThreshold = 1 * time.Millisecond
	topic.PublishSettings.CountThreshold = 1

	sub, err := client.CreateSubscription(ctx, uniqueName("bench-e2e-sub"), pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		b.Fatalf("CreateSubscription: %v", err)
	}
	b.Cleanup(func() { _ = sub.Delete(context.Background()) })
	sub.ReceiveSettings.MaxOutstandingMessages = 1

	// Channel used to signal that a message was received and acked.
	received := make(chan struct{}, 1)

	receiveCtx, cancelReceive := context.WithCancel(ctx)
	b.Cleanup(cancelReceive)

	go func() {
		_ = sub.Receive(receiveCtx, func(_ context.Context, msg *pubsub.Message) {
			msg.Ack()
			received <- struct{}{}
		})
	}()

	payload := []byte("e2e-payload")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := topic.Publish(ctx, &pubsub.Message{Data: payload}).Get(ctx); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		var done atomic.Bool
		select {
		case <-received:
			done.Store(true)
		case <-time.After(10 * time.Second):
		}
		if !done.Load() {
			b.Fatal("message not received within 10s")
		}
	}
}
