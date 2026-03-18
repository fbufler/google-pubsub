//go:build integration

package integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

func TestDeadLetter_MaxDeliveryAttempts(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	topic := mustCreateTopic(t, client, uniqueName("topic"))
	dlqTopic := mustCreateTopic(t, client, uniqueName("dlq-topic"))
	dlqSub := mustCreateSubscription(t, client, uniqueName("dlq-sub"), dlqTopic)

	const maxAttempts = 5
	sub, err := client.CreateSubscription(ctx, uniqueName("sub"), pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: maxAttempts,
		},
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	t.Cleanup(func() { _ = sub.Delete(context.Background()) })

	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("fail-me")}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Nack every delivery on the main sub
	var attempts int32
	subCtx, subCancel := context.WithTimeout(ctx, 60*time.Second)
	defer subCancel()
	go func() {
		_ = sub.Receive(subCtx, func(ctx context.Context, msg *pubsub.Message) {
			atomic.AddInt32(&attempts, 1)
			msg.Nack()
		})
	}()

	// Wait for the message to arrive on the dead-letter topic
	received := false
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()

	err = dlqSub.Receive(dlqCtx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		received = true
		dlqCancel()
		subCancel()
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("dlq Receive: %v", err)
	}
	if !received {
		t.Error("message never reached dead-letter topic")
	}
}
