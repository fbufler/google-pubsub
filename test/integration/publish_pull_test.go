//go:build integration

package integration_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

func TestPublish_SingleMessage(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	mustCreateSubscription(t, client, uniqueName("sub"), topic)

	result := topic.Publish(ctx, &pubsub.Message{
		Data:       []byte("hello"),
		Attributes: map[string]string{"key": "val"},
	})
	msgID, err := result.Get(ctx)
	if err != nil {
		t.Fatalf("Publish.Get: %v", err)
	}
	if msgID == "" {
		t.Error("empty message ID returned")
	}
}

func TestPublish_BatchMessages(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	mustCreateSubscription(t, client, uniqueName("sub"), topic)

	const n = 10
	results := make([]*pubsub.PublishResult, n)
	for i := range results {
		results[i] = topic.Publish(ctx, &pubsub.Message{Data: []byte("msg")})
	}
	for i, r := range results {
		if _, err := r.Get(ctx); err != nil {
			t.Errorf("message %d: %v", i, err)
		}
	}
}

func TestPull_ReceiveMessages(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	want := []string{"alpha", "beta", "gamma"}
	for _, body := range want {
		if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte(body)}).Get(ctx); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	var received sync.Map
	var count int32 // Atomic counter for tracking received messages

	recvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err := sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		received.Store(string(msg.Data), true)
		n := atomic.AddInt32(&count, 1)
		msg.Ack()

		if int(n) >= len(want) {
			cancel()
		}
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}

	for _, body := range want {
		if _, ok := received.Load(body); !ok {
			t.Errorf("message %q not received", body)
		}
	}
}

func TestPull_Nack_Redelivery(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub, err := client.CreateSubscription(ctx, uniqueName("sub"), pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	t.Cleanup(func() { _ = sub.Delete(context.Background()) })

	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("nack-me")}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var deliveries atomic.Int32
	recvCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err = sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		n := deliveries.Add(1)
		if n == 1 {
			msg.Nack()
			return
		}
		msg.Ack()
		cancel()
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}
	if deliveries.Load() < 2 {
		t.Errorf("expected at least 2 deliveries (nack + redelivery), got %d", deliveries.Load())
	}
}

func TestPull_MessageAttributes(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	attrs := map[string]string{"color": "blue", "size": "large"}
	if _, err := topic.Publish(ctx, &pubsub.Message{
		Data:       []byte("with-attrs"),
		Attributes: attrs,
	}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	recvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err := sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		defer msg.Ack()
		defer cancel()
		for k, v := range attrs {
			if msg.Attributes[k] != v {
				t.Errorf("attr[%q] = %q, want %q", k, msg.Attributes[k], v)
			}
		}
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}
}

func TestPull_OrderingKey(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	topic, err := client.CreateTopicWithConfig(ctx, uniqueName("topic"), &pubsub.TopicConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	t.Cleanup(func() { _ = topic.Delete(context.Background()) })
	topic.EnableMessageOrdering = true

	sub, err := client.CreateSubscription(ctx, uniqueName("sub"), pubsub.SubscriptionConfig{
		Topic:                 topic,
		AckDeadline:           10 * time.Second,
		EnableMessageOrdering: true,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	t.Cleanup(func() { _ = sub.Delete(context.Background()) })

	const key = "order-key"
	bodies := []string{"first", "second", "third"}
	for _, b := range bodies {
		if _, err := topic.Publish(ctx, &pubsub.Message{
			Data:        []byte(b),
			OrderingKey: key,
		}).Get(ctx); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Create a buffered channel to hold the results
	gotCh := make(chan string, len(bodies))

	recvCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// sub.Receive blocks until the context is canceled or an error occurs
	go func() {
		err = sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
			gotCh <- string(msg.Data)
			msg.Ack()

			// If we've hit our count, stop receiving
			if len(gotCh) == len(bodies) {
				cancel()
			}
		})
	}()

	<-recvCtx.Done()

	// Collect from channel
	var got []string
	close(gotCh) // Close so we can range over it
	for msg := range gotCh {
		got = append(got)
		got = append(got, msg)
	}

	// Validation: No sorting! We want to check the actual order.
	if len(got) != len(bodies) {
		t.Fatalf("expected %d messages, got %d", len(bodies), len(got))
	}

	for i, want := range bodies {
		if got[i] != want {
			t.Errorf("at index %d: got %q, want %q (Ordering failed!)", i, got[i], want)
		}
	}
}
