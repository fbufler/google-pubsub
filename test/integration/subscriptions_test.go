//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubscription_CreateAndGet(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	cfg, err := sub.Config(ctx)
	if err != nil {
		t.Fatalf("sub.Config: %v", err)
	}
	if cfg.Topic.ID() != topic.ID() {
		t.Errorf("topic ID = %q, want %q", cfg.Topic.ID(), topic.ID())
	}
	if cfg.AckDeadline != 10*time.Second {
		t.Errorf("AckDeadline = %v, want 10s", cfg.AckDeadline)
	}
}

func TestSubscription_CreateDuplicate(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	id := uniqueName("sub-dup")

	mustCreateSubscription(t, client, id, topic)
	_, err := client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{Topic: topic})
	if err == nil {
		t.Fatal("expected error creating duplicate subscription, got nil")
	}
	if st, ok := status.FromError(err); ok {
		if st.Code() != codes.AlreadyExists {
			t.Errorf("code = %v, want AlreadyExists", st.Code())
		}
	}
}

func TestSubscription_Delete(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	id := uniqueName("sub-del")

	sub, err := client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if err := sub.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// re-create should succeed
	sub2, err := client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	_ = sub2.Delete(ctx)
}

func TestSubscription_List(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))

	ids := []string{uniqueName("sl-a"), uniqueName("sl-b")}
	for _, id := range ids {
		mustCreateSubscription(t, client, id, topic)
	}

	found := make(map[string]bool)
	it := client.Subscriptions(ctx)
	for {
		s, err := it.Next()
		if err != nil {
			break
		}
		found[s.ID()] = true
	}

	for _, id := range ids {
		if !found[id] {
			t.Errorf("subscription %q not found in list", id)
		}
	}
}

func TestSubscription_UpdateAckDeadline(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	cfg, err := sub.Update(ctx, pubsub.SubscriptionConfigToUpdate{
		AckDeadline: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("sub.Update: %v", err)
	}
	if cfg.AckDeadline != 30*time.Second {
		t.Errorf("AckDeadline = %v, want 30s", cfg.AckDeadline)
	}
}

func TestSubscription_CreateOnNonExistentTopic(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ghostTopic := client.Topic("does-not-exist")
	_, err := client.CreateSubscription(ctx, uniqueName("sub"), pubsub.SubscriptionConfig{
		Topic: ghostTopic,
	})
	if err == nil {
		t.Fatal("expected error creating subscription on non-existent topic, got nil")
	}
}
