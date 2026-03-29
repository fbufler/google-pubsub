//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTopic_CreateAndGet(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	id := uniqueName("topic")

	topic := mustCreateTopic(t, client, id)
	if topic.ID() != id {
		t.Errorf("topic ID = %q, want %q", topic.ID(), id)
	}

	cfg, err := topic.Config(ctx)
	if err != nil {
		t.Fatalf("topic.Config: %v", err)
	}
	_ = cfg
}

func TestTopic_CreateDuplicate(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	id := uniqueName("topic-dup")

	mustCreateTopic(t, client, id)

	_, err := client.CreateTopic(ctx, id)
	if err == nil {
		t.Fatal("expected error creating duplicate topic, got nil")
	}
	if st, ok := status.FromError(err); ok {
		if st.Code() != codes.AlreadyExists {
			t.Errorf("code = %v, want AlreadyExists", st.Code())
		}
	}
}

func TestTopic_Delete(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	id := uniqueName("topic-del")

	topic, err := client.CreateTopic(ctx, id)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := topic.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = client.CreateTopic(ctx, id) // should succeed after delete
	if err != nil {
		t.Fatalf("CreateTopic after delete: %v", err)
	}
	_ = topic.Delete(ctx)
}

func TestTopic_List(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ids := []string{uniqueName("list-a"), uniqueName("list-b"), uniqueName("list-c")}
	for _, id := range ids {
		mustCreateTopic(t, client, id)
	}

	found := make(map[string]bool)
	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err != nil {
			break
		}
		found[topic.ID()] = true
	}

	for _, id := range ids {
		if !found[id] {
			t.Errorf("topic %q not found in list", id)
		}
	}
}

func TestTopic_UpdateRetentionDuration(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	id := uniqueName("topic-upd")

	topic := mustCreateTopic(t, client, id)
	retention := 24 * time.Hour
	cfg, err := topic.Update(ctx, pubsub.TopicConfigToUpdate{
		RetentionDuration: retention,
	})
	if err != nil {
		t.Fatalf("topic.Update: %v", err)
	}
	if cfg.RetentionDuration != retention {
		t.Errorf("RetentionDuration = %v, want %v", cfg.RetentionDuration, retention)
	}
}

func TestTopic_ListSnapshots(t *testing.T) {
	client := newClient(t)
	raw := newRawPublisherClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic-lsnap"))
	sub := mustCreateSubscription(t, client, uniqueName("sub-lsnap"), topic)

	snapID := uniqueName("snap-lsnap")
	if _, err := sub.CreateSnapshot(ctx, snapID); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snapName := "projects/" + projectID() + "/snapshots/" + snapID
	t.Cleanup(func() { _ = client.Snapshot(snapID).Delete(context.Background()) })

	it := raw.ListTopicSnapshots(ctx, &pubsubpb.ListTopicSnapshotsRequest{
		Topic: fqTopic(topic),
	})
	found := false
	for {
		name, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("ListTopicSnapshots: %v", err)
		}
		if name == snapName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("snapshot %q not listed under topic", snapName)
	}
}


func TestTopic_ListSubscriptions(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topicID := uniqueName("topic-listsubs")

	topic := mustCreateTopic(t, client, topicID)
	sub1 := mustCreateSubscription(t, client, uniqueName("sub"), topic)
	sub2 := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	found := make(map[string]bool)
	it := topic.Subscriptions(ctx)
	for {
		s, err := it.Next()
		if err != nil {
			break
		}
		found[s.ID()] = true
	}

	for _, s := range []*pubsub.Subscription{sub1, sub2} {
		if !found[s.ID()] {
			t.Errorf("subscription %q not found under topic", s.ID())
		}
	}
}
