//go:build integration

package integration_test

import (
	"context"
	"testing"

	pubsubapiv1 "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func newRESTPublisher(t *testing.T) *pubsubapiv1.PublisherClient {
	t.Helper()
	ctx := context.Background()
	client, err := pubsubapiv1.NewPublisherRESTClient(ctx,
		option.WithEndpoint("http://"+emulatorHost()),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewPublisherRESTClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestRESTGateway_CreateAndListTopics(t *testing.T) {
	ctx := context.Background()
	client := newRESTPublisher(t)
	topicName := "projects/" + projectID() + "/topics/" + uniqueName("rest-topic")

	created, err := client.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if created.Name != topicName {
		t.Errorf("got topic name %q, want %q", created.Name, topicName)
	}

	it := client.ListTopics(ctx, &pubsubpb.ListTopicsRequest{
		Project: "projects/" + projectID(),
	})
	found := false
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("ListTopics.Next: %v", err)
		}
		if topic.Name == topicName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("created topic %q not found in REST list", topicName)
	}
}

func TestRESTGateway_PublishMessage(t *testing.T) {
	ctx := context.Background()
	client := newRESTPublisher(t)
	topicName := "projects/" + projectID() + "/topics/" + uniqueName("rest-pub-topic")

	if _, err := client.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	resp, err := client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topicName,
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("hello")}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(resp.MessageIds) == 0 {
		t.Error("expected at least one message ID, got none")
	}
}
