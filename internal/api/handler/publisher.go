package handler

import (
	"context"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

var _ pubsubpbconnect.PublisherHandler = (*Publisher)(nil)

// Publisher implements the PubSub Publisher gRPC service by delegating each
// method to an independently injectable handler function.
type Publisher struct {
	pubsubpbconnect.UnimplementedPublisherHandler
	createTopic            func(context.Context, *connect.Request[pubsubpb.Topic]) (*connect.Response[pubsubpb.Topic], error)
	getTopic               func(context.Context, *connect.Request[pubsubpb.GetTopicRequest]) (*connect.Response[pubsubpb.Topic], error)
	updateTopic            func(context.Context, *connect.Request[pubsubpb.UpdateTopicRequest]) (*connect.Response[pubsubpb.Topic], error)
	deleteTopic            func(context.Context, *connect.Request[pubsubpb.DeleteTopicRequest]) (*connect.Response[emptypb.Empty], error)
	listTopics             func(context.Context, *connect.Request[pubsubpb.ListTopicsRequest]) (*connect.Response[pubsubpb.ListTopicsResponse], error)
	listTopicSubscriptions func(context.Context, *connect.Request[pubsubpb.ListTopicSubscriptionsRequest]) (*connect.Response[pubsubpb.ListTopicSubscriptionsResponse], error)
	listTopicSnapshots     func(context.Context, *connect.Request[pubsubpb.ListTopicSnapshotsRequest]) (*connect.Response[pubsubpb.ListTopicSnapshotsResponse], error)
	publish                func(context.Context, *connect.Request[pubsubpb.PublishRequest]) (*connect.Response[pubsubpb.PublishResponse], error)
	detachSubscription     func(context.Context, *connect.Request[pubsubpb.DetachSubscriptionRequest]) (*connect.Response[pubsubpb.DetachSubscriptionResponse], error)
}

func NewPublisher(ctx context.Context, topicUC *usecases.TopicUsecase, pubUC *usecases.PublisherUsecase) *Publisher {
	return &Publisher{
		createTopic:            NewCreateTopic(ctx, topicUC),
		getTopic:               NewGetTopic(ctx, topicUC),
		updateTopic:            NewUpdateTopic(ctx, topicUC),
		deleteTopic:            NewDeleteTopic(ctx, topicUC),
		listTopics:             NewListTopics(ctx, topicUC),
		listTopicSubscriptions: NewListTopicSubscriptions(ctx, topicUC),
		listTopicSnapshots:     NewListTopicSnapshots(ctx, topicUC),
		publish:                NewPublish(ctx, pubUC),
		detachSubscription:     NewDetachSubscription(ctx, topicUC),
	}
}

func (p *Publisher) CreateTopic(ctx context.Context, req *connect.Request[pubsubpb.Topic]) (*connect.Response[pubsubpb.Topic], error) {
	return p.createTopic(ctx, req)
}

func (p *Publisher) GetTopic(ctx context.Context, req *connect.Request[pubsubpb.GetTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	return p.getTopic(ctx, req)
}

func (p *Publisher) UpdateTopic(ctx context.Context, req *connect.Request[pubsubpb.UpdateTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	return p.updateTopic(ctx, req)
}

func (p *Publisher) DeleteTopic(ctx context.Context, req *connect.Request[pubsubpb.DeleteTopicRequest]) (*connect.Response[emptypb.Empty], error) {
	return p.deleteTopic(ctx, req)
}

func (p *Publisher) ListTopics(ctx context.Context, req *connect.Request[pubsubpb.ListTopicsRequest]) (*connect.Response[pubsubpb.ListTopicsResponse], error) {
	return p.listTopics(ctx, req)
}

func (p *Publisher) ListTopicSubscriptions(ctx context.Context, req *connect.Request[pubsubpb.ListTopicSubscriptionsRequest]) (*connect.Response[pubsubpb.ListTopicSubscriptionsResponse], error) {
	return p.listTopicSubscriptions(ctx, req)
}

func (p *Publisher) ListTopicSnapshots(ctx context.Context, req *connect.Request[pubsubpb.ListTopicSnapshotsRequest]) (*connect.Response[pubsubpb.ListTopicSnapshotsResponse], error) {
	return p.listTopicSnapshots(ctx, req)
}

func (p *Publisher) Publish(ctx context.Context, req *connect.Request[pubsubpb.PublishRequest]) (*connect.Response[pubsubpb.PublishResponse], error) {
	return p.publish(ctx, req)
}

func (p *Publisher) DetachSubscription(ctx context.Context, req *connect.Request[pubsubpb.DetachSubscriptionRequest]) (*connect.Response[pubsubpb.DetachSubscriptionResponse], error) {
	return p.detachSubscription(ctx, req)
}
