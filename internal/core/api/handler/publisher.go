package handler

import (
	"context"
	"fmt"
	"log/slog"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

var _ pubsubpbconnect.PublisherHandler = (*Publisher)(nil)

// Publisher implements the PubSub Publisher gRPC service.
type Publisher struct {
	pubsubpbconnect.UnimplementedPublisherHandler
	topic   *usecases.TopicUsecase
	publish *usecases.PublisherUsecase
}

func NewPublisher(topic *usecases.TopicUsecase, publish *usecases.PublisherUsecase) *Publisher {
	return &Publisher{topic: topic, publish: publish}
}

func (p *Publisher) CreateTopic(_ context.Context, req *connect.Request[pubsubpb.Topic]) (*connect.Response[pubsubpb.Topic], error) {
	t := req.Msg
	if t.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic name is required"))
	}
	topic := new(entities.Topic)
	if err := topic.SetName(types.FQDN(t.Name)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if len(t.Labels) > 0 {
		if err := topic.SetLabels(types.Labels(t.Labels)); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	if t.MessageRetentionDuration != nil {
		if err := topic.SetMessageRetention(t.MessageRetentionDuration.AsDuration()); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	if err := p.topic.CreateTopic(topic); err != nil {
		if err == types.ErrAlreadyExists {
			slog.Debug("create topic: already exists", "topic", t.Name)
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("topic %q already exists", t.Name))
		}
		slog.Error("create topic: internal error", "topic", t.Name, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("topic created", "topic", t.Name)
	return connect.NewResponse(topicToProto(topic)), nil
}

func (p *Publisher) GetTopic(_ context.Context, req *connect.Request[pubsubpb.GetTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	topic, err := p.topic.GetTopic(types.FQDN(req.Msg.Topic))
	if err != nil {
		if err == types.ErrNotFound {
			slog.Debug("get topic: not found", "topic", req.Msg.Topic)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		slog.Error("get topic: internal error", "topic", req.Msg.Topic, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("topic retrieved", "topic", req.Msg.Topic)
	return connect.NewResponse(topicToProto(topic)), nil
}

func (p *Publisher) UpdateTopic(_ context.Context, req *connect.Request[pubsubpb.UpdateTopicRequest]) (*connect.Response[pubsubpb.Topic], error) {
	if req.Msg.Topic == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
	}
	existing, err := p.topic.GetTopic(types.FQDN(req.Msg.Topic.Name))
	if err != nil {
		if err == types.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, path := range req.Msg.UpdateMask.GetPaths() {
		switch path {
		case "labels":
			if err := existing.SetLabels(types.Labels(req.Msg.Topic.Labels)); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
		case "message_retention_duration":
			if req.Msg.Topic.MessageRetentionDuration != nil {
				if err := existing.SetMessageRetention(req.Msg.Topic.MessageRetentionDuration.AsDuration()); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
			}
		}
	}
	if err := p.topic.UpdateTopic(existing); err != nil {
		slog.Error("update topic: internal error", "topic", req.Msg.Topic.Name, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("topic updated", "topic", req.Msg.Topic.Name)
	return connect.NewResponse(topicToProto(existing)), nil
}

func (p *Publisher) DeleteTopic(_ context.Context, req *connect.Request[pubsubpb.DeleteTopicRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := p.topic.DeleteTopic(types.FQDN(req.Msg.Topic)); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("delete topic: not found", "topic", req.Msg.Topic)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		slog.Error("delete topic: internal error", "topic", req.Msg.Topic, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("topic deleted", "topic", req.Msg.Topic)
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (p *Publisher) ListTopics(_ context.Context, req *connect.Request[pubsubpb.ListTopicsRequest]) (*connect.Response[pubsubpb.ListTopicsResponse], error) {
	topics, err := p.topic.ListTopics(projectID(req.Msg.Project))
	if err != nil {
		slog.Error("list topics: internal error", "project", req.Msg.Project, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &pubsubpb.ListTopicsResponse{}
	for _, t := range topics {
		resp.Topics = append(resp.Topics, topicToProto(t))
	}
	slog.Debug("topics listed", "project", req.Msg.Project, "count", len(resp.Topics))
	return connect.NewResponse(resp), nil
}

func (p *Publisher) ListTopicSubscriptions(_ context.Context, req *connect.Request[pubsubpb.ListTopicSubscriptionsRequest]) (*connect.Response[pubsubpb.ListTopicSubscriptionsResponse], error) {
	names, err := p.topic.ListTopicSubscriptions(types.FQDN(req.Msg.Topic))
	if err != nil {
		if err == types.ErrNotFound {
			slog.Debug("list topic subscriptions: topic not found", "topic", req.Msg.Topic)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		slog.Error("list topic subscriptions: internal error", "topic", req.Msg.Topic, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	strNames := make([]string, len(names))
	for i, n := range names {
		strNames[i] = n.String()
	}
	slog.Debug("topic subscriptions listed", "topic", req.Msg.Topic, "count", len(strNames))
	return connect.NewResponse(&pubsubpb.ListTopicSubscriptionsResponse{Subscriptions: strNames}), nil
}

func (p *Publisher) ListTopicSnapshots(_ context.Context, req *connect.Request[pubsubpb.ListTopicSnapshotsRequest]) (*connect.Response[pubsubpb.ListTopicSnapshotsResponse], error) {
	snaps, err := p.topic.ListTopicSnapshots(types.FQDN(req.Msg.Topic))
	if err != nil {
		slog.Error("list topic snapshots: internal error", "topic", req.Msg.Topic, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	names := make([]string, len(snaps))
	for i, s := range snaps {
		names[i] = s.Name().String()
	}
	slog.Debug("topic snapshots listed", "topic", req.Msg.Topic, "count", len(names))
	return connect.NewResponse(&pubsubpb.ListTopicSnapshotsResponse{Snapshots: names}), nil
}

func (p *Publisher) Publish(_ context.Context, req *connect.Request[pubsubpb.PublishRequest]) (*connect.Response[pubsubpb.PublishResponse], error) {
	if req.Msg.Topic == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
	}
	msgs := make([]*entities.Message, 0, len(req.Msg.Messages))
	for _, m := range req.Msg.Messages {
		msg := new(entities.Message)
		if err := msg.SetData(m.Data); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		if err := msg.SetAttributes(m.Attributes); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		if err := msg.SetOrderingKey(m.OrderingKey); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		msgs = append(msgs, msg)
	}
	ids, err := p.publish.Publish(types.FQDN(req.Msg.Topic), msgs)
	if err != nil {
		if err == types.ErrNotFound {
			slog.Debug("publish: topic not found", "topic", req.Msg.Topic)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", req.Msg.Topic))
		}
		slog.Error("publish: internal error", "topic", req.Msg.Topic, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("messages published", "topic", req.Msg.Topic, "count", len(ids))
	return connect.NewResponse(&pubsubpb.PublishResponse{MessageIds: ids}), nil
}

func (p *Publisher) DetachSubscription(_ context.Context, req *connect.Request[pubsubpb.DetachSubscriptionRequest]) (*connect.Response[pubsubpb.DetachSubscriptionResponse], error) {
	if err := p.topic.DetachSubscription(types.FQDN(req.Msg.Subscription)); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("detach subscription: not found", "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("detach subscription: internal error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("subscription detached", "subscription", req.Msg.Subscription)
	return connect.NewResponse(&pubsubpb.DetachSubscriptionResponse{}), nil
}

// --- proto converters ---

func topicToProto(t *entities.Topic) *pubsubpb.Topic {
	pt := &pubsubpb.Topic{
		Name:   t.Name().String(),
		Labels: map[string]string(t.Labels()),
	}
	if t.MessageRetention() > 0 {
		pt.MessageRetentionDuration = durationpb.New(t.MessageRetention())
	}
	return pt
}

