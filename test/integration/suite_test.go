//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	pubsubapiv1 "cloud.google.com/go/pubsub/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// emulatorHost returns the host:port of the emulator under test.
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

// newClient creates a PubSub client connected to the emulator.
func newClient(t *testing.T) *pubsub.Client {
	t.Helper()
	ctx := context.Background()
	host := emulatorHost()
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%q): %v", host, err)
	}
	t.Cleanup(func() { conn.Close() })

	client, err := pubsub.NewClient(ctx, projectID(),
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("pubsub.NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// uniqueName returns a unique resource name with the given prefix.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// mustCreateTopic creates a topic and registers cleanup.
func mustCreateTopic(t *testing.T, client *pubsub.Client, id string) *pubsub.Topic {
	t.Helper()
	ctx := context.Background()
	topic, err := client.CreateTopic(ctx, id)
	if err != nil {
		t.Fatalf("CreateTopic(%q): %v", id, err)
	}
	t.Cleanup(func() { _ = topic.Delete(context.Background()) })
	return topic
}

// mustCreateSubscription creates a subscription and registers cleanup.
func mustCreateSubscription(t *testing.T, client *pubsub.Client, id string, topic *pubsub.Topic) *pubsub.Subscription {
	t.Helper()
	ctx := context.Background()
	sub, err := client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("CreateSubscription(%q): %v", id, err)
	}
	t.Cleanup(func() { _ = sub.Delete(context.Background()) })
	return sub
}

// mustPublish publishes a single message and fails the test on error.
func mustPublish(t *testing.T, topic *pubsub.Topic, data string) {
	t.Helper()
	if _, err := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(data)}).Get(context.Background()); err != nil {
		t.Fatalf("Publish %q: %v", data, err)
	}
}

// fqSub returns the fully-qualified subscription resource name.
func fqSub(sub *pubsub.Subscription) string {
	return "projects/" + projectID() + "/subscriptions/" + sub.ID()
}

// fqTopic returns the fully-qualified topic resource name.
func fqTopic(topic *pubsub.Topic) string {
	return "projects/" + projectID() + "/topics/" + topic.ID()
}

// newRawSubscriberClient returns a low-level gRPC subscriber client connected to the emulator.
func newRawSubscriberClient(t *testing.T) *pubsubapiv1.SubscriberClient {
	t.Helper()
	conn, err := grpc.NewClient(emulatorHost(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	client, err := pubsubapiv1.NewSubscriberClient(context.Background(),
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewSubscriberClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// newRawPublisherClient returns a low-level gRPC publisher client connected to the emulator.
func newRawPublisherClient(t *testing.T) *pubsubapiv1.PublisherClient {
	t.Helper()
	conn, err := grpc.NewClient(emulatorHost(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	client, err := pubsubapiv1.NewPublisherClient(context.Background(),
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewPublisherClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}
