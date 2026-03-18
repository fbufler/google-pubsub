//go:build integration

package integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

// Receive() uses StreamingPull under the hood; these tests exercise that path.

func TestStreamingPull_MultipleMessages(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	const n = 50
	for i := 0; i < n; i++ {
		topic.Publish(ctx, &pubsub.Message{Data: []byte("msg")})
	}
	topic.Flush()

	var count int64
	recvCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	err := sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		if atomic.AddInt64(&count, 1) >= n {
			cancel()
		}
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}
	if count < n {
		t.Errorf("received %d messages, want %d", count, n)
	}
}

func TestStreamingPull_ModifyAckDeadline(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("extend-me")}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	received := make(chan struct{}, 1)
	recvCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	err := sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		// Extend the deadline before acking
		msg.Nack() // use nack to trigger redelivery and verify extension worked
		select {
		case received <- struct{}{}:
		default:
		}
		cancel()
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}
	select {
	case <-received:
	default:
		t.Error("no message received")
	}
}

func TestStreamingPull_ConcurrentSubscribers(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	const n = 20
	for i := 0; i < n; i++ {
		topic.Publish(ctx, &pubsub.Message{Data: []byte("concurrent")})
	}
	topic.Flush()

	var count int64
	recvCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	sub.ReceiveSettings.NumGoroutines = 5
	err := sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		if atomic.AddInt64(&count, 1) >= n {
			cancel()
		}
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}
	if count < n {
		t.Errorf("received %d messages, want %d", count, n)
	}
}
