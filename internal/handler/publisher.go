package handler

import (
	"context"
	"fmt"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/domain"
	"github.com/fbufler/google-pubsub/internal/storage"
)

var _ pubsubpbconnect.PublisherHandler = (*Publisher)(nil)

// Publisher implements the PubSub Publisher gRPC service.
type Publisher struct {
	pubsubpbconnect.UnimplementedPublisherHandler
	store *storage.Store
}

func NewPublisher(store *storage.Store) *Publisher {
	return &Publisher{store: store}
}

func (p *Publisher) CreateTopic(_ context.Context, req *connect.Request[pubsubpb.Topic]) (*connect.Response[pubsubpb.Topic], error) {
	t := req.Msg
	if t.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic name is required"))
	}
	topic := &domain.Topic{
		Name:      t.Name,
		Labels:    t.Labels,
		CreatedAt: time.Now(),
	}
	if t.MessageRetentionDuration != nil {
		topic.MessageRetention = t.MessageRetentionDuration.AsDuration()
	}
	if err := p.store.CreateTopic(topic); err != nil {
		if err == domain.ErrAlreadyExists {
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("topic %q already exists", t.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(topicToProto(topic)), nil
}

func (p *Publisher) GetTopic(_ context.Context, req *connect.Request[pubsubpb.GetTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	topic, err := p.store.GetTopic(req.Msg.Topic)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(topicToProto(topic)), nil
}

func (p *Publisher) UpdateTopic(_ context.Context, req *connect.Request[pubsubpb.UpdateTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	if req.Msg.Topic == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
	}
	existing, err := p.store.GetTopic(req.Msg.Topic.Name)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, path := range req.Msg.UpdateMask.GetPaths() {
		switch path {
		case "labels":
			existing.Labels = req.Msg.Topic.Labels
		case "message_retention_duration":
			if req.Msg.Topic.MessageRetentionDuration != nil {
				existing.MessageRetention = req.Msg.Topic.MessageRetentionDuration.AsDuration()
			}
		}
	}
	if err := p.store.UpdateTopic(existing); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(topicToProto(existing)), nil
}

func (p *Publisher) DeleteTopic(_ context.Context, req *connect.Request[pubsubpb.DeleteTopicRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := p.store.DeleteTopic(req.Msg.Topic); err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (p *Publisher) ListTopics(_ context.Context, req *connect.Request[pubsubpb.ListTopicsRequest]) (*connect.Response[pubsubpb.ListTopicsResponse], error) {
	project := projectID(req.Msg.Project)
	topics, err := p.store.ListTopics(project)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &pubsubpb.ListTopicsResponse{}
	for _, t := range topics {
		resp.Topics = append(resp.Topics, topicToProto(t))
	}
	return connect.NewResponse(resp), nil
}

func (p *Publisher) ListTopicSubscriptions(_ context.Context, req *connect.Request[pubsubpb.ListTopicSubscriptionsRequest]) (*connect.Response[pubsubpb.ListTopicSubscriptionsResponse], error) {
	names, err := p.store.ListTopicSubscriptions(req.Msg.Topic)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pubsubpb.ListTopicSubscriptionsResponse{Subscriptions: names}), nil
}

func (p *Publisher) ListTopicSnapshots(_ context.Context, req *connect.Request[pubsubpb.ListTopicSnapshotsRequest]) (*connect.Response[pubsubpb.ListTopicSnapshotsResponse], error) {
	snaps, err := p.store.ListSnapshotsByTopic(req.Msg.Topic)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	var names []string
	for _, s := range snaps {
		names = append(names, s.Name)
	}
	return connect.NewResponse(&pubsubpb.ListTopicSnapshotsResponse{Snapshots: names}), nil
}

func (p *Publisher) Publish(_ context.Context, req *connect.Request[pubsubpb.PublishRequest]) (*connect.Response[pubsubpb.PublishResponse], error) {
	if req.Msg.Topic == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
	}
	now := time.Now()
	msgs := make([]*domain.Message, 0, len(req.Msg.Messages))
	ids := make([]string, 0, len(req.Msg.Messages))
	for _, m := range req.Msg.Messages {
		id := storage.NewMsgID()
		ids = append(ids, id)
		msgs = append(msgs, &domain.Message{
			ID:          id,
			Data:        m.Data,
			Attributes:  m.Attributes,
			OrderingKey: m.OrderingKey,
			PublishTime: now,
		})
	}
	if err := p.store.AppendMessages(req.Msg.Topic, msgs); err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pubsubpb.PublishResponse{MessageIds: ids}), nil
}

func (p *Publisher) DetachSubscription(_ context.Context, req *connect.Request[pubsubpb.DetachSubscriptionRequest]) (*connect.Response[pubsubpb.DetachSubscriptionResponse], error) {
	sub, err := p.store.GetSubscription(req.Msg.Subscription)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	sub.TopicName = "_deleted-topic_"
	if err := p.store.UpdateSubscription(sub); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pubsubpb.DetachSubscriptionResponse{}), nil
}

func topicToProto(t *domain.Topic) *pubsubpb.Topic {
	pt := &pubsubpb.Topic{
		Name:   t.Name,
		Labels: t.Labels,
	}
	if t.MessageRetention > 0 {
		pt.MessageRetentionDuration = durationpb.New(t.MessageRetention)
	}
	return pt
}
