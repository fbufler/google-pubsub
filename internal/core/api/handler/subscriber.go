package handler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

var _ pubsubpbconnect.SubscriberHandler = (*Subscriber)(nil)

// Subscriber implements the PubSub Subscriber gRPC service.
type Subscriber struct {
	pubsubpbconnect.UnimplementedSubscriberHandler
	sub  *usecases.SubscriberUsecase
	snap *usecases.SnapshotUsecase
}

func NewSubscriber(sub *usecases.SubscriberUsecase, snap *usecases.SnapshotUsecase) *Subscriber {
	return &Subscriber{sub: sub, snap: snap}
}

func (s *Subscriber) CreateSubscription(_ context.Context, req *connect.Request[pubsubpb.Subscription]) (*connect.Response[pubsubpb.Subscription], error) {
	p := req.Msg
	if p.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription name is required"))
	}
	if p.Topic == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
	}

	sub := new(entities.Subscription)
	if err := sub.SetName(types.FQDN(p.Name)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := sub.SetTopicName(types.FQDN(p.Topic)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if len(p.Labels) > 0 {
		if err := sub.SetLabels(types.Labels(p.Labels)); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	ackDeadline := 10 * time.Second
	if p.AckDeadlineSeconds > 0 {
		ackDeadline = time.Duration(p.AckDeadlineSeconds) * time.Second
	}
	if err := sub.SetAckDeadline(ackDeadline); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	_ = sub.SetRetainAckedMessages(p.RetainAckedMessages)

	msgRetention := 7 * 24 * time.Hour
	if p.MessageRetentionDuration != nil {
		msgRetention = p.MessageRetentionDuration.AsDuration()
	}
	if err := sub.SetMessageRetention(msgRetention); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if err := sub.SetFilter(p.Filter); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	_ = sub.SetEnableMessageOrdering(p.EnableMessageOrdering)
	_ = sub.SetEnableExactlyOnceDelivery(p.EnableExactlyOnceDelivery)

	if p.DeadLetterPolicy != nil {
		if err := sub.SetDeadLetterPolicy(&types.DeadLetterPolicy{
			DeadLetterTopic:     p.DeadLetterPolicy.DeadLetterTopic,
			MaxDeliveryAttempts: p.DeadLetterPolicy.MaxDeliveryAttempts,
		}); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	if p.RetryPolicy != nil {
		rp := &types.RetryPolicy{}
		if p.RetryPolicy.MinimumBackoff != nil {
			rp.MinimumBackoff = p.RetryPolicy.MinimumBackoff.AsDuration()
		}
		if p.RetryPolicy.MaximumBackoff != nil {
			rp.MaximumBackoff = p.RetryPolicy.MaximumBackoff.AsDuration()
		}
		if err := sub.SetRetryPolicy(rp); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	if p.PushConfig != nil {
		_ = sub.SetPushConfig(&types.PushConfig{
			Endpoint:   p.PushConfig.PushEndpoint,
			Attributes: p.PushConfig.Attributes,
		})
	}

	if err := s.sub.CreateSubscription(sub); err != nil {
		if err == types.ErrAlreadyExists {
			slog.Debug("create subscription: already exists", "subscription", p.Name)
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("subscription %q already exists", p.Name))
		}
		if err == types.ErrNotFound {
			slog.Debug("create subscription: topic not found", "subscription", p.Name, "topic", p.Topic)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", p.Topic))
		}
		slog.Error("create subscription: internal error", "subscription", p.Name, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("subscription created", "subscription", p.Name, "topic", p.Topic)
	return connect.NewResponse(subToProto(sub)), nil
}

func (s *Subscriber) GetSubscription(_ context.Context, req *connect.Request[pubsubpb.GetSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	sub, err := s.sub.GetSubscription(types.FQDN(req.Msg.Subscription))
	if err != nil {
		if err == types.ErrNotFound {
			slog.Debug("get subscription: not found", "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("get subscription: internal error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("subscription retrieved", "subscription", req.Msg.Subscription)
	return connect.NewResponse(subToProto(sub)), nil
}

func (s *Subscriber) UpdateSubscription(_ context.Context, req *connect.Request[pubsubpb.UpdateSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	if req.Msg.Subscription == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription is required"))
	}
	existing, err := s.sub.GetSubscription(types.FQDN(req.Msg.Subscription.Name))
	if err != nil {
		if err == types.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, path := range req.Msg.UpdateMask.GetPaths() {
		switch path {
		case "ack_deadline_seconds":
			if err := existing.SetAckDeadline(time.Duration(req.Msg.Subscription.AckDeadlineSeconds) * time.Second); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
		case "labels":
			if err := existing.SetLabels(types.Labels(req.Msg.Subscription.Labels)); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
		case "message_retention_duration":
			if req.Msg.Subscription.MessageRetentionDuration != nil {
				if err := existing.SetMessageRetention(req.Msg.Subscription.MessageRetentionDuration.AsDuration()); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
			}
		case "retain_acked_messages":
			_ = existing.SetRetainAckedMessages(req.Msg.Subscription.RetainAckedMessages)
		case "push_config":
			if req.Msg.Subscription.PushConfig != nil {
				_ = existing.SetPushConfig(&types.PushConfig{
					Endpoint:   req.Msg.Subscription.PushConfig.PushEndpoint,
					Attributes: req.Msg.Subscription.PushConfig.Attributes,
				})
			}
		case "dead_letter_policy":
			if req.Msg.Subscription.DeadLetterPolicy != nil {
				if err := existing.SetDeadLetterPolicy(&types.DeadLetterPolicy{
					DeadLetterTopic:     req.Msg.Subscription.DeadLetterPolicy.DeadLetterTopic,
					MaxDeliveryAttempts: req.Msg.Subscription.DeadLetterPolicy.MaxDeliveryAttempts,
				}); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
			}
		case "retry_policy":
			if req.Msg.Subscription.RetryPolicy != nil {
				rp := &types.RetryPolicy{}
				if req.Msg.Subscription.RetryPolicy.MinimumBackoff != nil {
					rp.MinimumBackoff = req.Msg.Subscription.RetryPolicy.MinimumBackoff.AsDuration()
				}
				if req.Msg.Subscription.RetryPolicy.MaximumBackoff != nil {
					rp.MaximumBackoff = req.Msg.Subscription.RetryPolicy.MaximumBackoff.AsDuration()
				}
				if err := existing.SetRetryPolicy(rp); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
			}
		}
	}
	if err := s.sub.UpdateSubscription(existing); err != nil {
		slog.Error("update subscription: internal error", "subscription", req.Msg.Subscription.Name, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("subscription updated", "subscription", req.Msg.Subscription.Name)
	return connect.NewResponse(subToProto(existing)), nil
}

func (s *Subscriber) DeleteSubscription(_ context.Context, req *connect.Request[pubsubpb.DeleteSubscriptionRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := s.sub.DeleteSubscription(types.FQDN(req.Msg.Subscription)); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("delete subscription: not found", "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("delete subscription: internal error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("subscription deleted", "subscription", req.Msg.Subscription)
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ListSubscriptions(_ context.Context, req *connect.Request[pubsubpb.ListSubscriptionsRequest]) (*connect.Response[pubsubpb.ListSubscriptionsResponse], error) {
	subs, err := s.sub.ListSubscriptions(projectID(req.Msg.Project))
	if err != nil {
		slog.Error("list subscriptions: internal error", "project", req.Msg.Project, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &pubsubpb.ListSubscriptionsResponse{}
	for _, sub := range subs {
		resp.Subscriptions = append(resp.Subscriptions, subToProto(sub))
	}
	slog.Debug("subscriptions listed", "project", req.Msg.Project, "count", len(resp.Subscriptions))
	return connect.NewResponse(resp), nil
}

func (s *Subscriber) Pull(_ context.Context, req *connect.Request[pubsubpb.PullRequest]) (*connect.Response[pubsubpb.PullResponse], error) {
	max := req.Msg.MaxMessages
	if max <= 0 {
		max = 100
	}
	subName := types.FQDN(req.Msg.Subscription)
	msgs, err := s.sub.Pull(subName, max)
	if err != nil {
		if err == types.ErrNotFound {
			slog.Debug("pull: subscription not found", "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("pull: internal error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	msgs, err = s.sub.HandleDeadLetters(subName, msgs)
	if err != nil {
		slog.Error("pull: dead letter handling error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &pubsubpb.PullResponse{}
	for _, pm := range msgs {
		resp.ReceivedMessages = append(resp.ReceivedMessages, pendingToProto(pm))
	}
	slog.Debug("messages pulled", "subscription", req.Msg.Subscription, "count", len(resp.ReceivedMessages))
	return connect.NewResponse(resp), nil
}

func (s *Subscriber) Acknowledge(_ context.Context, req *connect.Request[pubsubpb.AcknowledgeRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := s.sub.Acknowledge(types.FQDN(req.Msg.Subscription), req.Msg.AckIds); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("acknowledge: subscription not found", "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("acknowledge: internal error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("messages acknowledged", "subscription", req.Msg.Subscription, "count", len(req.Msg.AckIds))
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ModifyAckDeadline(_ context.Context, req *connect.Request[pubsubpb.ModifyAckDeadlineRequest]) (*connect.Response[emptypb.Empty], error) {
	deadline := time.Duration(req.Msg.AckDeadlineSeconds) * time.Second
	if err := s.sub.ModifyAckDeadline(types.FQDN(req.Msg.Subscription), req.Msg.AckIds, deadline); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("modify ack deadline: subscription not found", "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("modify ack deadline: internal error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("ack deadline modified", "subscription", req.Msg.Subscription, "count", len(req.Msg.AckIds), "deadline", deadline)
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ModifyPushConfig(_ context.Context, req *connect.Request[pubsubpb.ModifyPushConfigRequest]) (*connect.Response[emptypb.Empty], error) {
	var cfg *types.PushConfig
	if req.Msg.PushConfig != nil {
		cfg = &types.PushConfig{
			Endpoint:   req.Msg.PushConfig.PushEndpoint,
			Attributes: req.Msg.PushConfig.Attributes,
		}
	}
	if err := s.sub.ModifyPushConfig(types.FQDN(req.Msg.Subscription), cfg); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("modify push config: subscription not found", "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("modify push config: internal error", "subscription", req.Msg.Subscription, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("push config modified", "subscription", req.Msg.Subscription)
	return connect.NewResponse(&emptypb.Empty{}), nil
}

// StreamingPull implements bidirectional streaming.
func (s *Subscriber) StreamingPull(ctx context.Context, stream *connect.BidiStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error {
	initReq, err := stream.Receive()
	if err != nil {
		return err
	}
	subName := types.FQDN(initReq.Subscription)
	if subName == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription is required"))
	}
	if _, err := s.sub.GetSubscription(subName); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("streaming pull: subscription not found", "subscription", subName)
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", subName))
		}
		slog.Error("streaming pull: internal error", "subscription", subName, "err", err)
		return connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("streaming pull connected", "subscription", subName)

	if err := s.processStreamReq(subName, initReq); err != nil {
		return err
	}

	incomingCh := make(chan *pubsubpb.StreamingPullRequest, 64)
	errCh := make(chan error, 1)

	go func() {
		for {
			req, err := stream.Receive()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case incomingCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-errCh:
			return err

		case req := <-incomingCh:
			if err := s.processStreamReq(subName, req); err != nil {
				return err
			}

		case <-ticker.C:
			msgs, err := s.sub.Pull(subName, 100)
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			msgs, err = s.sub.HandleDeadLetters(subName, msgs)
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			if len(msgs) == 0 {
				continue
			}
			resp := &pubsubpb.StreamingPullResponse{}
			for _, pm := range msgs {
				resp.ReceivedMessages = append(resp.ReceivedMessages, pendingToProto(pm))
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (s *Subscriber) processStreamReq(subName types.FQDN, req *pubsubpb.StreamingPullRequest) error {
	if len(req.AckIds) > 0 {
		if err := s.sub.Acknowledge(subName, req.AckIds); err != nil && err != types.ErrNotFound {
			return connect.NewError(connect.CodeInternal, err)
		}
	}
	if len(req.ModifyDeadlineAckIds) > 0 && len(req.ModifyDeadlineSeconds) > 0 {
		for i, ackID := range req.ModifyDeadlineAckIds {
			var sec int32
			if i < len(req.ModifyDeadlineSeconds) {
				sec = req.ModifyDeadlineSeconds[i]
			}
			_ = s.sub.ModifyAckDeadline(subName, []string{ackID}, time.Duration(sec)*time.Second)
		}
	}
	return nil
}

// --- Snapshot methods ---

func (s *Subscriber) CreateSnapshot(_ context.Context, req *connect.Request[pubsubpb.CreateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	snap := new(entities.Snapshot)
	if err := snap.SetName(types.FQDN(req.Msg.Name)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := snap.SetSubscriptionName(types.FQDN(req.Msg.Subscription)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if len(req.Msg.Labels) > 0 {
		if err := snap.SetLabels(types.Labels(req.Msg.Labels)); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	if err := s.snap.CreateSnapshot(snap); err != nil {
		if err == types.ErrAlreadyExists {
			slog.Debug("create snapshot: already exists", "snapshot", req.Msg.Name)
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("snapshot %q already exists", req.Msg.Name))
		}
		if err == types.ErrNotFound {
			slog.Debug("create snapshot: subscription not found", "snapshot", req.Msg.Name, "subscription", req.Msg.Subscription)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		slog.Error("create snapshot: internal error", "snapshot", req.Msg.Name, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("snapshot created", "snapshot", req.Msg.Name, "subscription", req.Msg.Subscription)
	return connect.NewResponse(snapshotToProto(snap)), nil
}

func (s *Subscriber) GetSnapshot(_ context.Context, req *connect.Request[pubsubpb.GetSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	snap, err := s.snap.GetSnapshot(types.FQDN(req.Msg.Snapshot))
	if err != nil {
		if err == types.ErrNotFound {
			slog.Debug("get snapshot: not found", "snapshot", req.Msg.Snapshot)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("snapshot %q not found", req.Msg.Snapshot))
		}
		slog.Error("get snapshot: internal error", "snapshot", req.Msg.Snapshot, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("snapshot retrieved", "snapshot", req.Msg.Snapshot)
	return connect.NewResponse(snapshotToProto(snap)), nil
}

func (s *Subscriber) UpdateSnapshot(_ context.Context, req *connect.Request[pubsubpb.UpdateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	if req.Msg.Snapshot == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("snapshot is required"))
	}
	existing, err := s.snap.GetSnapshot(types.FQDN(req.Msg.Snapshot.Name))
	if err != nil {
		if err == types.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("snapshot %q not found", req.Msg.Snapshot.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, path := range req.Msg.UpdateMask.GetPaths() {
		switch path {
		case "labels":
			if err := existing.SetLabels(types.Labels(req.Msg.Snapshot.Labels)); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
		case "expire_time":
			if req.Msg.Snapshot.ExpireTime != nil {
				if err := existing.RestoreExpireTime(req.Msg.Snapshot.ExpireTime.AsTime()); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
			}
		}
	}
	if err := s.snap.UpdateSnapshot(existing); err != nil {
		slog.Error("update snapshot: internal error", "snapshot", req.Msg.Snapshot.Name, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("snapshot updated", "snapshot", req.Msg.Snapshot.Name)
	return connect.NewResponse(snapshotToProto(existing)), nil
}

func (s *Subscriber) DeleteSnapshot(_ context.Context, req *connect.Request[pubsubpb.DeleteSnapshotRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := s.snap.DeleteSnapshot(types.FQDN(req.Msg.Snapshot)); err != nil {
		if err == types.ErrNotFound {
			slog.Debug("delete snapshot: not found", "snapshot", req.Msg.Snapshot)
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("snapshot %q not found", req.Msg.Snapshot))
		}
		slog.Error("delete snapshot: internal error", "snapshot", req.Msg.Snapshot, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("snapshot deleted", "snapshot", req.Msg.Snapshot)
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ListSnapshots(_ context.Context, req *connect.Request[pubsubpb.ListSnapshotsRequest]) (*connect.Response[pubsubpb.ListSnapshotsResponse], error) {
	snaps, err := s.snap.ListSnapshots(projectID(req.Msg.Project))
	if err != nil {
		slog.Error("list snapshots: internal error", "project", req.Msg.Project, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &pubsubpb.ListSnapshotsResponse{}
	for _, snap := range snaps {
		resp.Snapshots = append(resp.Snapshots, snapshotToProto(snap))
	}
	slog.Debug("snapshots listed", "project", req.Msg.Project, "count", len(resp.Snapshots))
	return connect.NewResponse(resp), nil
}

func (s *Subscriber) Seek(_ context.Context, req *connect.Request[pubsubpb.SeekRequest]) (*connect.Response[pubsubpb.SeekResponse], error) {
	subName := types.FQDN(req.Msg.Subscription)
	switch target := req.Msg.Target.(type) {
	case *pubsubpb.SeekRequest_Time:
		if err := s.snap.SeekToTime(subName, target.Time.AsTime()); err != nil {
			if err == types.ErrNotFound {
				slog.Debug("seek: subscription not found", "subscription", req.Msg.Subscription)
				return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
			}
			slog.Error("seek: internal error", "subscription", req.Msg.Subscription, "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		slog.Debug("subscription seeked to time", "subscription", req.Msg.Subscription, "time", target.Time.AsTime())
	case *pubsubpb.SeekRequest_Snapshot:
		if err := s.snap.SeekToSnapshot(subName, types.FQDN(target.Snapshot)); err != nil {
			if err == types.ErrNotFound {
				slog.Debug("seek: resource not found", "subscription", req.Msg.Subscription, "snapshot", target.Snapshot)
				return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("resource not found"))
			}
			slog.Error("seek: internal error", "subscription", req.Msg.Subscription, "snapshot", target.Snapshot, "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		slog.Debug("subscription seeked to snapshot", "subscription", req.Msg.Subscription, "snapshot", target.Snapshot)
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("seek target is required"))
	}
	return connect.NewResponse(&pubsubpb.SeekResponse{}), nil
}

// --- proto converters ---

func subToProto(sub *entities.Subscription) *pubsubpb.Subscription {
	p := &pubsubpb.Subscription{
		Name:                      sub.Name().String(),
		Topic:                     sub.TopicName().String(),
		Labels:                    map[string]string(sub.Labels()),
		AckDeadlineSeconds:        int32(sub.AckDeadline().Seconds()),
		RetainAckedMessages:       sub.RetainAckedMessages(),
		MessageRetentionDuration:  durationpb.New(sub.MessageRetention()),
		Filter:                    sub.Filter(),
		EnableMessageOrdering:     sub.EnableMessageOrdering(),
		EnableExactlyOnceDelivery: sub.EnableExactlyOnceDelivery(),
	}
	if sub.DeadLetterPolicy() != nil {
		p.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     sub.DeadLetterPolicy().DeadLetterTopic,
			MaxDeliveryAttempts: sub.DeadLetterPolicy().MaxDeliveryAttempts,
		}
	}
	if sub.RetryPolicy() != nil {
		p.RetryPolicy = &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(sub.RetryPolicy().MinimumBackoff),
			MaximumBackoff: durationpb.New(sub.RetryPolicy().MaximumBackoff),
		}
	}
	if sub.PushConfig() != nil {
		p.PushConfig = &pubsubpb.PushConfig{
			PushEndpoint: sub.PushConfig().Endpoint,
			Attributes:   sub.PushConfig().Attributes,
		}
	}
	return p
}

func snapshotToProto(snap *entities.Snapshot) *pubsubpb.Snapshot {
	p := &pubsubpb.Snapshot{
		Name:   snap.Name().String(),
		Topic:  snap.TopicName().String(),
		Labels: map[string]string(snap.Labels()),
	}
	if !snap.ExpireTime().IsZero() {
		p.ExpireTime = timestamppb.New(snap.ExpireTime())
	}
	return p
}

func pendingToProto(pm *entities.PendingMessage) *pubsubpb.ReceivedMessage {
	return &pubsubpb.ReceivedMessage{
		AckId:           pm.AckID(),
		DeliveryAttempt: pm.DeliveryAttempt(),
		Message: &pubsubpb.PubsubMessage{
			MessageId:   pm.Message().ID(),
			Data:        pm.Message().Data(),
			Attributes:  pm.Message().Attributes(),
			OrderingKey: pm.Message().OrderingKey(),
			PublishTime: timestamppb.New(pm.Message().PublishTime()),
		},
	}
}
