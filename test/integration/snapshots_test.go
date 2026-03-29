//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
)

func TestSnapshot_CreateAndList(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	// publish a message so there's something to snapshot
	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("snap")}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	snapID := uniqueName("snap")
	_, err := sub.CreateSnapshot(ctx, snapID)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snap := client.Snapshot(snapID)
	t.Cleanup(func() { _ = snap.Delete(context.Background()) })

	found := false
	it := client.Snapshots(ctx)
	for {
		s, err := it.Next()
		if err != nil {
			break
		}
		if s.ID() == snapID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("snapshot %q not found in list", snapID)
	}
}

func TestSnapshot_Seek(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	// publish and consume one message
	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("before-snap")}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		cancel()
	})

	// take a snapshot at this point
	snapID := uniqueName("snap")
	if _, err := sub.CreateSnapshot(ctx, snapID); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snap := client.Snapshot(snapID)
	t.Cleanup(func() { _ = snap.Delete(context.Background()) })

	// publish a second message
	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("after-snap")}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// seek back to snapshot — should replay messages published after snapshot
	if err := sub.SeekToSnapshot(ctx, snap); err != nil {
		t.Fatalf("SeekToSnapshot: %v", err)
	}

	received := false
	recvCtx2, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	defer cancel2()
	err := sub.Receive(recvCtx2, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		if string(msg.Data) == "after-snap" {
			received = true
		}
		cancel2()
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive after seek: %v", err)
	}
	if !received {
		t.Error("did not receive expected message after seek to snapshot")
	}
}

func TestSnapshot_SeekToTime(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	// SeekToTime requires message retention on the topic
	topic, err := client.CreateTopicWithConfig(ctx, uniqueName("topic"), &pubsub.TopicConfig{
		RetentionDuration: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateTopicWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = topic.Delete(context.Background()) })

	sub, err := client.CreateSubscription(ctx, uniqueName("sub"), pubsub.SubscriptionConfig{
		Topic:                   topic,
		AckDeadline:             10 * time.Second,
		RetainAckedMessages:     true,
		RetentionDuration:       24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	t.Cleanup(func() { _ = sub.Delete(context.Background()) })

	// publish, then record time, then publish again
	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("before")}).Get(ctx); err != nil {
		t.Fatalf("Publish before: %v", err)
	}

	seekTime := time.Now()
	time.Sleep(200 * time.Millisecond)

	if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("after")}).Get(ctx); err != nil {
		t.Fatalf("Publish after: %v", err)
	}

	// consume both
	var count int
	recvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_ = sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		count++
		if count >= 2 {
			cancel()
		}
	})

	// seek to the recorded time — should only replay "after"
	if err := sub.SeekToTime(ctx, seekTime); err != nil {
		t.Fatalf("SeekToTime: %v", err)
	}

	// collect all replayed messages for up to 10s, then verify
	received := make(map[string]int)
	recvCtx2, cancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel2()
	var seekErr error
	seekErr = sub.Receive(recvCtx2, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		received[string(msg.Data)]++
	})
	if seekErr != nil && seekErr != context.DeadlineExceeded && seekErr != context.Canceled {
		t.Fatalf("Receive after seek: %v", seekErr)
	}

	if received["after"] == 0 {
		t.Errorf("expected to receive 'after' message after time seek, got: %v", received)
	}
	if received["before"] > 0 {
		t.Errorf("'before' should not be replayed after time seek, got %d copies", received["before"])
	}
}

func TestSnapshot_Delete(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sub"), topic)

	snapID := uniqueName("snap-del")
	if _, err := sub.CreateSnapshot(ctx, snapID); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snap := client.Snapshot(snapID)

	if err := snap.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestSnapshot_Get(t *testing.T) {
	client := newClient(t)
	raw := newRawSubscriberClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("snap-get-topic"))
	sub := mustCreateSubscription(t, client, uniqueName("snap-get-sub"), topic)

	snapID := uniqueName("snap-get")
	if _, err := sub.CreateSnapshot(ctx, snapID); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snapName := "projects/" + projectID() + "/snapshots/" + snapID
	t.Cleanup(func() { _ = client.Snapshot(snapID).Delete(context.Background()) })

	got, err := raw.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: snapName})
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Name != snapName {
		t.Errorf("snapshot name = %q, want %q", got.Name, snapName)
	}
}


func TestSnapshot_ListByTopic(t *testing.T) {
	client := newClient(t)
	raw := newRawPublisherClient(t)
	ctx := context.Background()
	topic := mustCreateTopic(t, client, uniqueName("snap-lbt-topic"))
	sub := mustCreateSubscription(t, client, uniqueName("snap-lbt-sub"), topic)

	snapID := uniqueName("snap-lbt")
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
			t.Fatalf("ListTopicSnapshots.Next: %v", err)
		}
		if name == snapName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("snapshot %q not found under topic", snapName)
	}
}
