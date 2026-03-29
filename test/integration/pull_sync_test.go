//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSyncPull_ReceiveAndAck(t *testing.T) {
	client := newClient(t)
	raw := newRawSubscriberClient(t)
	ctx := context.Background()

	topic := mustCreateTopic(t, client, uniqueName("sp-topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sp-sub"), topic)
	mustPublish(t, topic, "sync-pull-msg")

	time.Sleep(50 * time.Millisecond)

	resp, err := raw.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: fqSub(sub),
		MaxMessages:  10,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(resp.ReceivedMessages) == 0 {
		t.Fatal("expected at least one message, got none")
	}
	got := string(resp.ReceivedMessages[0].Message.Data)
	if got != "sync-pull-msg" {
		t.Errorf("data = %q, want %q", got, "sync-pull-msg")
	}

	var ackIDs []string
	for _, m := range resp.ReceivedMessages {
		ackIDs = append(ackIDs, m.AckId)
	}
	if err := raw.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: fqSub(sub),
		AckIds:       ackIDs,
	}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
}

func TestSyncPull_EmptyQueue(t *testing.T) {
	client := newClient(t)
	raw := newRawSubscriberClient(t)

	topic := mustCreateTopic(t, client, uniqueName("sp-empty-topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sp-empty-sub"), topic)

	// Real PubSub long-polls on empty queues and returns DeadlineExceeded when the
	// context expires. Accept either that or an immediate empty response.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := raw.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: fqSub(sub),
		MaxMessages:  10,
	})
	if err != nil {
		if c := status.Code(err); c == codes.DeadlineExceeded || c == codes.Canceled {
			return // long-poll behaviour: expected
		}
		t.Fatalf("Pull on empty queue: %v", err)
	}
	if len(resp.ReceivedMessages) != 0 {
		t.Errorf("expected 0 messages from empty queue, got %d", len(resp.ReceivedMessages))
	}
}

func TestSyncPull_MaxMessages(t *testing.T) {
	client := newClient(t)
	raw := newRawSubscriberClient(t)
	ctx := context.Background()

	topic := mustCreateTopic(t, client, uniqueName("sp-max-topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sp-max-sub"), topic)

	for i := 0; i < 5; i++ {
		mustPublish(t, topic, "msg")
	}
	time.Sleep(50 * time.Millisecond)

	resp, err := raw.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: fqSub(sub),
		MaxMessages:  3,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(resp.ReceivedMessages) > 3 {
		t.Errorf("got %d messages, want at most 3", len(resp.ReceivedMessages))
	}
}

func TestSyncPull_ModifyAckDeadline(t *testing.T) {
	client := newClient(t)
	raw := newRawSubscriberClient(t)
	ctx := context.Background()

	topic := mustCreateTopic(t, client, uniqueName("sp-mack-topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sp-mack-sub"), topic)
	mustPublish(t, topic, "extend-me")

	time.Sleep(50 * time.Millisecond)

	resp, err := raw.Pull(ctx, &pubsubpb.PullRequest{Subscription: fqSub(sub), MaxMessages: 1})
	if err != nil || len(resp.ReceivedMessages) == 0 {
		t.Fatalf("Pull: err=%v msgs=%d", err, len(resp.ReceivedMessages))
	}
	ackID := resp.ReceivedMessages[0].AckId

	if err := raw.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       fqSub(sub),
		AckIds:             []string{ackID},
		AckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("ModifyAckDeadline: %v", err)
	}

	if err := raw.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: fqSub(sub),
		AckIds:       []string{ackID},
	}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
}

func TestSyncPull_ModifyPushConfig(t *testing.T) {
	client := newClient(t)
	raw := newRawSubscriberClient(t)
	ctx := context.Background()

	topic := mustCreateTopic(t, client, uniqueName("sp-mpc-topic"))
	sub := mustCreateSubscription(t, client, uniqueName("sp-mpc-sub"), topic)

	// Set a push endpoint.
	if err := raw.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{
		Subscription: fqSub(sub),
		PushConfig:   &pubsubpb.PushConfig{PushEndpoint: "https://example.com/push"},
	}); err != nil {
		t.Fatalf("ModifyPushConfig (set): %v", err)
	}

	// Clear it back to pull.
	if err := raw.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{
		Subscription: fqSub(sub),
		PushConfig:   &pubsubpb.PushConfig{},
	}); err != nil {
		t.Fatalf("ModifyPushConfig (clear): %v", err)
	}
}
