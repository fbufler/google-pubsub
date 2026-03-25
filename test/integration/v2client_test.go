//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pubsubv2 "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// newClientV2 creates a v2 PubSub client connected to the emulator under test.
// emulatorHost() and projectID() are shared helpers from suite_test.go.
func newClientV2(t *testing.T) *pubsubv2.Client {
	t.Helper()
	host := emulatorHost()
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%q): %v", host, err)
	}
	t.Cleanup(func() { conn.Close() })

	client, err := pubsubv2.NewClient(context.Background(), projectID(),
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("pubsubv2.NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// v2TopicName returns the fully-qualified topic name for the test project.
func v2TopicName(id string) string {
	return fmt.Sprintf("projects/%s/topics/%s", projectID(), id)
}

// v2SubName returns the fully-qualified subscription name for the test project.
func v2SubName(id string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", projectID(), id)
}

func mustCreateTopicV2(t *testing.T, client *pubsubv2.Client, id string) string {
	t.Helper()
	name := v2TopicName(id)
	_, err := client.TopicAdminClient.CreateTopic(context.Background(), &pubsubpb.Topic{Name: name})
	if err != nil {
		t.Fatalf("v2 CreateTopic(%q): %v", id, err)
	}
	t.Cleanup(func() {
		_ = client.TopicAdminClient.DeleteTopic(context.Background(), &pubsubpb.DeleteTopicRequest{Topic: name})
	})
	return name
}

func mustCreateSubscriptionV2(t *testing.T, client *pubsubv2.Client, subID, topicName string) string {
	t.Helper()
	name := v2SubName(subID)
	_, err := client.SubscriptionAdminClient.CreateSubscription(context.Background(), &pubsubpb.Subscription{
		Name:               name,
		Topic:              topicName,
		AckDeadlineSeconds: 10,
	})
	if err != nil {
		t.Fatalf("v2 CreateSubscription(%q): %v", subID, err)
	}
	t.Cleanup(func() {
		_ = client.SubscriptionAdminClient.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{Subscription: name})
	})
	return name
}

// --- Tests ---

func TestV2Client_Publish_SingleMessage(t *testing.T) {
	client := newClientV2(t)
	ctx := context.Background()
	topicName := mustCreateTopicV2(t, client, uniqueName("v2-topic"))
	mustCreateSubscriptionV2(t, client, uniqueName("v2-sub"), topicName)

	pub := client.Publisher(topicName)
	defer pub.Stop()

	msgID, err := pub.Publish(ctx, &pubsubv2.Message{Data: []byte("hello from v2")}).Get(ctx)
	if err != nil {
		t.Fatalf("Publish.Get: %v", err)
	}
	if msgID == "" {
		t.Error("empty message ID returned")
	}
}

func TestV2Client_Publish_BatchMessages(t *testing.T) {
	client := newClientV2(t)
	ctx := context.Background()
	topicName := mustCreateTopicV2(t, client, uniqueName("v2-topic"))
	mustCreateSubscriptionV2(t, client, uniqueName("v2-sub"), topicName)

	pub := client.Publisher(topicName)
	defer pub.Stop()

	const n = 10
	results := make([]*pubsubv2.PublishResult, n)
	for i := range results {
		results[i] = pub.Publish(ctx, &pubsubv2.Message{Data: []byte("msg")})
	}
	for i, r := range results {
		if _, err := r.Get(ctx); err != nil {
			t.Errorf("message %d: %v", i, err)
		}
	}
}

func TestV2Client_Receive_Messages(t *testing.T) {
	client := newClientV2(t)
	ctx := context.Background()
	topicName := mustCreateTopicV2(t, client, uniqueName("v2-topic"))
	subName := mustCreateSubscriptionV2(t, client, uniqueName("v2-sub"), topicName)

	pub := client.Publisher(topicName)
	defer pub.Stop()

	want := []string{"alpha", "beta", "gamma"}
	for _, body := range want {
		if _, err := pub.Publish(ctx, &pubsubv2.Message{Data: []byte(body)}).Get(ctx); err != nil {
			t.Fatalf("Publish %q: %v", body, err)
		}
	}

	var received sync.Map
	var count int32 // Atomic counter for tracking received messages

	recvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	sub := client.Subscriber(subName)
	err := sub.Receive(recvCtx, func(_ context.Context, msg *pubsubv2.Message) {
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

func TestV2Client_Nack_Redelivery(t *testing.T) {
	client := newClientV2(t)
	ctx := context.Background()
	topicName := mustCreateTopicV2(t, client, uniqueName("v2-topic"))
	subName := mustCreateSubscriptionV2(t, client, uniqueName("v2-sub"), topicName)

	pub := client.Publisher(topicName)
	defer pub.Stop()

	if _, err := pub.Publish(ctx, &pubsubv2.Message{Data: []byte("nack-me")}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var deliveries atomic.Int32
	recvCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	sub := client.Subscriber(subName)
	err := sub.Receive(recvCtx, func(_ context.Context, msg *pubsubv2.Message) {
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

func TestV2Client_MessageAttributes(t *testing.T) {
	client := newClientV2(t)
	ctx := context.Background()
	topicName := mustCreateTopicV2(t, client, uniqueName("v2-topic"))
	subName := mustCreateSubscriptionV2(t, client, uniqueName("v2-sub"), topicName)

	pub := client.Publisher(topicName)
	defer pub.Stop()

	attrs := map[string]string{"env": "test", "version": "2"}
	if _, err := pub.Publish(ctx, &pubsubv2.Message{
		Data:       []byte("with-attrs"),
		Attributes: attrs,
	}).Get(ctx); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	recvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	sub := client.Subscriber(subName)
	err := sub.Receive(recvCtx, func(_ context.Context, msg *pubsubv2.Message) {
		defer msg.Ack()
		defer cancel()
		for k, v := range attrs {
			if got := msg.Attributes[k]; got != v {
				t.Errorf("attr[%q] = %q, want %q", k, got, v)
			}
		}
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Receive: %v", err)
	}
}

func TestV2Client_TopicLifecycle(t *testing.T) {
	client := newClientV2(t)
	ctx := context.Background()
	name := v2TopicName(uniqueName("v2-topic"))

	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if _, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name}); err != nil {
		t.Fatalf("GetTopic after create: %v", err)
	}

	if err := client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: name}); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	if _, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name}); err == nil {
		t.Error("expected error getting deleted topic, got nil")
	}
}

func TestV2Client_SubscriptionLifecycle(t *testing.T) {
	client := newClientV2(t)
	ctx := context.Background()
	topicName := mustCreateTopicV2(t, client, uniqueName("v2-topic"))
	subName := v2SubName(uniqueName("v2-sub"))

	_, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               subName,
		Topic:              topicName,
		AckDeadlineSeconds: 20,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	if _, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subName}); err != nil {
		t.Fatalf("GetSubscription after create: %v", err)
	}

	if err := client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: subName}); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}

	if _, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subName}); err == nil {
		t.Error("expected error getting deleted subscription, got nil")
	}
}
