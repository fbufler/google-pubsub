package handler

import (
	"context"
	"fmt"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/domain"
	"github.com/fbufler/google-pubsub/internal/storage"
)

var _ pubsubpbconnect.SubscriberHandler = (*Subscriber)(nil)

// Subscriber implements the PubSub Subscriber gRPC service.
type Subscriber struct {
	pubsubpbconnect.UnimplementedSubscriberHandler
	store *storage.Store
}

func NewSubscriber(store *storage.Store) *Subscriber {
	return &Subscriber{store: store}
}

func (s *Subscriber) CreateSubscription(_ context.Context, req *connect.Request[pubsubpb.Subscription]) (*connect.Response[pubsubpb.Subscription], error) {
	p := req.Msg
	if p.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription name is required"))
	}
	if p.Topic == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic is required"))
	}
	// Verify topic exists
	if _, err := s.store.GetTopic(p.Topic); err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("topic %q not found", p.Topic))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	ackDeadline := 10 * time.Second
	if p.AckDeadlineSeconds > 0 {
		ackDeadline = time.Duration(p.AckDeadlineSeconds) * time.Second
	}
	msgRetention := 7 * 24 * time.Hour
	if p.MessageRetentionDuration != nil {
		msgRetention = p.MessageRetentionDuration.AsDuration()
	}

	sub := &domain.Subscription{
		Name:                      p.Name,
		TopicName:                 p.Topic,
		Labels:                    p.Labels,
		AckDeadline:               ackDeadline,
		RetainAckedMessages:       p.RetainAckedMessages,
		MessageRetention:          msgRetention,
		Filter:                    p.Filter,
		EnableMessageOrdering:     p.EnableMessageOrdering,
		EnableExactlyOnceDelivery: p.EnableExactlyOnceDelivery,
		CreatedAt:                 time.Now(),
	}
	if p.DeadLetterPolicy != nil {
		sub.DeadLetterPolicy = &domain.DeadLetterPolicy{
			DeadLetterTopic:     p.DeadLetterPolicy.DeadLetterTopic,
			MaxDeliveryAttempts: p.DeadLetterPolicy.MaxDeliveryAttempts,
		}
	}
	if p.RetryPolicy != nil {
		sub.RetryPolicy = &domain.RetryPolicy{}
		if p.RetryPolicy.MinimumBackoff != nil {
			sub.RetryPolicy.MinimumBackoff = p.RetryPolicy.MinimumBackoff.AsDuration()
		}
		if p.RetryPolicy.MaximumBackoff != nil {
			sub.RetryPolicy.MaximumBackoff = p.RetryPolicy.MaximumBackoff.AsDuration()
		}
	}
	if p.PushConfig != nil {
		sub.PushConfig = &domain.PushConfig{
			Endpoint:   p.PushConfig.PushEndpoint,
			Attributes: p.PushConfig.Attributes,
		}
	}

	if err := s.store.CreateSubscription(sub); err != nil {
		if err == domain.ErrAlreadyExists {
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("subscription %q already exists", p.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(subToProto(sub)), nil
}

func (s *Subscriber) GetSubscription(_ context.Context, req *connect.Request[pubsubpb.GetSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	sub, err := s.store.GetSubscription(req.Msg.Subscription)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(subToProto(sub)), nil
}

func (s *Subscriber) UpdateSubscription(_ context.Context, req *connect.Request[pubsubpb.UpdateSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	if req.Msg.Subscription == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription is required"))
	}
	existing, err := s.store.GetSubscription(req.Msg.Subscription.Name)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, path := range req.Msg.UpdateMask.GetPaths() {
		switch path {
		case "ack_deadline_seconds":
			existing.AckDeadline = time.Duration(req.Msg.Subscription.AckDeadlineSeconds) * time.Second
		case "labels":
			existing.Labels = req.Msg.Subscription.Labels
		case "message_retention_duration":
			if req.Msg.Subscription.MessageRetentionDuration != nil {
				existing.MessageRetention = req.Msg.Subscription.MessageRetentionDuration.AsDuration()
			}
		case "retain_acked_messages":
			existing.RetainAckedMessages = req.Msg.Subscription.RetainAckedMessages
		case "push_config":
			if req.Msg.Subscription.PushConfig != nil {
				existing.PushConfig = &domain.PushConfig{
					Endpoint:   req.Msg.Subscription.PushConfig.PushEndpoint,
					Attributes: req.Msg.Subscription.PushConfig.Attributes,
				}
			}
		case "dead_letter_policy":
			if req.Msg.Subscription.DeadLetterPolicy != nil {
				existing.DeadLetterPolicy = &domain.DeadLetterPolicy{
					DeadLetterTopic:     req.Msg.Subscription.DeadLetterPolicy.DeadLetterTopic,
					MaxDeliveryAttempts: req.Msg.Subscription.DeadLetterPolicy.MaxDeliveryAttempts,
				}
			}
		case "retry_policy":
			if req.Msg.Subscription.RetryPolicy != nil {
				existing.RetryPolicy = &domain.RetryPolicy{}
				if req.Msg.Subscription.RetryPolicy.MinimumBackoff != nil {
					existing.RetryPolicy.MinimumBackoff = req.Msg.Subscription.RetryPolicy.MinimumBackoff.AsDuration()
				}
				if req.Msg.Subscription.RetryPolicy.MaximumBackoff != nil {
					existing.RetryPolicy.MaximumBackoff = req.Msg.Subscription.RetryPolicy.MaximumBackoff.AsDuration()
				}
			}
		}
	}
	if err := s.store.UpdateSubscription(existing); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(subToProto(existing)), nil
}

func (s *Subscriber) DeleteSubscription(_ context.Context, req *connect.Request[pubsubpb.DeleteSubscriptionRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := s.store.DeleteSubscription(req.Msg.Subscription); err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ListSubscriptions(_ context.Context, req *connect.Request[pubsubpb.ListSubscriptionsRequest]) (*connect.Response[pubsubpb.ListSubscriptionsResponse], error) {
	project := projectID(req.Msg.Project)
	subs, err := s.store.ListSubscriptions(project)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &pubsubpb.ListSubscriptionsResponse{}
	for _, sub := range subs {
		resp.Subscriptions = append(resp.Subscriptions, subToProto(sub))
	}
	return connect.NewResponse(resp), nil
}

func (s *Subscriber) Pull(_ context.Context, req *connect.Request[pubsubpb.PullRequest]) (*connect.Response[pubsubpb.PullResponse], error) {
	max := req.Msg.MaxMessages
	if max <= 0 {
		max = 100
	}
	msgs, err := s.store.PullPending(req.Msg.Subscription, max)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Handle dead letter forwarding
	msgs, err = s.handleDeadLetters(req.Msg.Subscription, msgs)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &pubsubpb.PullResponse{}
	for _, pm := range msgs {
		resp.ReceivedMessages = append(resp.ReceivedMessages, pendingToProto(pm))
	}
	return connect.NewResponse(resp), nil
}

func (s *Subscriber) Acknowledge(_ context.Context, req *connect.Request[pubsubpb.AcknowledgeRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := s.store.Acknowledge(req.Msg.Subscription, req.Msg.AckIds); err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ModifyAckDeadline(_ context.Context, req *connect.Request[pubsubpb.ModifyAckDeadlineRequest]) (*connect.Response[emptypb.Empty], error) {
	deadline := time.Duration(req.Msg.AckDeadlineSeconds) * time.Second
	if err := s.store.ModifyAckDeadline(req.Msg.Subscription, req.Msg.AckIds, deadline); err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ModifyPushConfig(_ context.Context, req *connect.Request[pubsubpb.ModifyPushConfigRequest]) (*connect.Response[emptypb.Empty], error) {
	sub, err := s.store.GetSubscription(req.Msg.Subscription)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if req.Msg.PushConfig != nil {
		sub.PushConfig = &domain.PushConfig{
			Endpoint:   req.Msg.PushConfig.PushEndpoint,
			Attributes: req.Msg.PushConfig.Attributes,
		}
	} else {
		sub.PushConfig = nil
	}
	if err := s.store.UpdateSubscription(sub); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}

// StreamingPull implements bidirectional streaming: the server continuously
// delivers available messages and the client sends acks/nacks.
func (s *Subscriber) StreamingPull(ctx context.Context, stream *connect.BidiStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error {
	// Read the initial request which carries the subscription name.
	initReq, err := stream.Receive()
	if err != nil {
		return err
	}
	subName := initReq.Subscription
	if subName == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscription is required"))
	}
	if _, err := s.store.GetSubscription(subName); err != nil {
		if err == domain.ErrNotFound {
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", subName))
		}
		return connect.NewError(connect.CodeInternal, err)
	}

	// Process initial acks/deadline modifications if present.
	if err := s.processStreamReq(subName, initReq); err != nil {
		return err
	}

	// Channel to receive incoming requests (acks, deadline mods).
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
			msgs, err := s.store.PullPending(subName, 100)
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			msgs, err = s.handleDeadLetters(subName, msgs)
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

func (s *Subscriber) processStreamReq(subName string, req *pubsubpb.StreamingPullRequest) error {
	if len(req.AckIds) > 0 {
		if err := s.store.Acknowledge(subName, req.AckIds); err != nil && err != domain.ErrNotFound {
			return connect.NewError(connect.CodeInternal, err)
		}
	}
	if len(req.ModifyDeadlineAckIds) > 0 && len(req.ModifyDeadlineSeconds) > 0 {
		for i, ackID := range req.ModifyDeadlineAckIds {
			var sec int32
			if i < len(req.ModifyDeadlineSeconds) {
				sec = req.ModifyDeadlineSeconds[i]
			}
			deadline := time.Duration(sec) * time.Second
			_ = s.store.ModifyAckDeadline(subName, []string{ackID}, deadline)
		}
	}
	return nil
}

// handleDeadLetters filters out messages exceeding the dead-letter threshold and
// forwards them to the dead-letter topic.
func (s *Subscriber) handleDeadLetters(subName string, msgs []*domain.PendingMessage) ([]*domain.PendingMessage, error) {
	sub, err := s.store.GetSubscription(subName)
	if err != nil || sub.DeadLetterPolicy == nil {
		return msgs, nil
	}
	max := sub.DeadLetterPolicy.MaxDeliveryAttempts
	dlqTopic := sub.DeadLetterPolicy.DeadLetterTopic

	var keep []*domain.PendingMessage
	var dlqMsgs []*domain.Message
	var dlqAckIDs []string

	for _, pm := range msgs {
		if max > 0 && pm.DeliveryAttempt >= max {
			dlqMsgs = append(dlqMsgs, pm.Message)
			dlqAckIDs = append(dlqAckIDs, pm.AckID)
		} else {
			keep = append(keep, pm)
		}
	}
	if len(dlqMsgs) > 0 {
		// Publish to DLQ topic (best-effort — ignore if topic not found).
		_ = s.store.AppendMessages(dlqTopic, dlqMsgs)
		_ = s.store.Acknowledge(subName, dlqAckIDs)
	}
	return keep, nil
}

// --- Snapshot methods ---

func (s *Subscriber) CreateSnapshot(_ context.Context, req *connect.Request[pubsubpb.CreateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	snap := &domain.Snapshot{
		Name:             req.Msg.Name,
		SubscriptionName: req.Msg.Subscription,
		Labels:           req.Msg.Labels,
	}
	if err := s.store.CreateSnapshotForSub(snap); err != nil {
		if err == domain.ErrAlreadyExists {
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("snapshot %q already exists", req.Msg.Name))
		}
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	saved, _ := s.store.GetSnapshot(req.Msg.Name)
	return connect.NewResponse(snapshotToProto(saved)), nil
}

func (s *Subscriber) GetSnapshot(_ context.Context, req *connect.Request[pubsubpb.GetSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	snap, err := s.store.GetSnapshot(req.Msg.Snapshot)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("snapshot %q not found", req.Msg.Snapshot))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(snapshotToProto(snap)), nil
}

func (s *Subscriber) UpdateSnapshot(_ context.Context, req *connect.Request[pubsubpb.UpdateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	if req.Msg.Snapshot == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("snapshot is required"))
	}
	existing, err := s.store.GetSnapshot(req.Msg.Snapshot.Name)
	if err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("snapshot %q not found", req.Msg.Snapshot.Name))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, path := range req.Msg.UpdateMask.GetPaths() {
		switch path {
		case "labels":
			existing.Labels = req.Msg.Snapshot.Labels
		case "expire_time":
			if req.Msg.Snapshot.ExpireTime != nil {
				existing.ExpireTime = req.Msg.Snapshot.ExpireTime.AsTime()
			}
		}
	}
	if err := s.store.UpdateSnapshot(existing); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(snapshotToProto(existing)), nil
}

func (s *Subscriber) DeleteSnapshot(_ context.Context, req *connect.Request[pubsubpb.DeleteSnapshotRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := s.store.DeleteSnapshot(req.Msg.Snapshot); err != nil {
		if err == domain.ErrNotFound {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("snapshot %q not found", req.Msg.Snapshot))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *Subscriber) ListSnapshots(_ context.Context, req *connect.Request[pubsubpb.ListSnapshotsRequest]) (*connect.Response[pubsubpb.ListSnapshotsResponse], error) {
	project := projectID(req.Msg.Project)
	snaps, err := s.store.ListSnapshots(project)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &pubsubpb.ListSnapshotsResponse{}
	for _, snap := range snaps {
		resp.Snapshots = append(resp.Snapshots, snapshotToProto(snap))
	}
	return connect.NewResponse(resp), nil
}

func (s *Subscriber) Seek(_ context.Context, req *connect.Request[pubsubpb.SeekRequest]) (*connect.Response[pubsubpb.SeekResponse], error) {
	switch target := req.Msg.Target.(type) {
	case *pubsubpb.SeekRequest_Time:
		if err := s.store.SeekToTime(req.Msg.Subscription, target.Time.AsTime()); err != nil {
			if err == domain.ErrNotFound {
				return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("subscription %q not found", req.Msg.Subscription))
			}
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	case *pubsubpb.SeekRequest_Snapshot:
		if err := s.store.SeekToSnapshot(req.Msg.Subscription, target.Snapshot); err != nil {
			if err == domain.ErrNotFound {
				return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("resource not found"))
			}
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("seek target is required"))
	}
	return connect.NewResponse(&pubsubpb.SeekResponse{}), nil
}

// --- proto converters ---

func subToProto(sub *domain.Subscription) *pubsubpb.Subscription {
	p := &pubsubpb.Subscription{
		Name:                      sub.Name,
		Topic:                     sub.TopicName,
		Labels:                    sub.Labels,
		AckDeadlineSeconds:        int32(sub.AckDeadline.Seconds()),
		RetainAckedMessages:       sub.RetainAckedMessages,
		MessageRetentionDuration:  durationpb.New(sub.MessageRetention),
		Filter:                    sub.Filter,
		EnableMessageOrdering:     sub.EnableMessageOrdering,
		EnableExactlyOnceDelivery: sub.EnableExactlyOnceDelivery,
	}
	if sub.DeadLetterPolicy != nil {
		p.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     sub.DeadLetterPolicy.DeadLetterTopic,
			MaxDeliveryAttempts: sub.DeadLetterPolicy.MaxDeliveryAttempts,
		}
	}
	if sub.RetryPolicy != nil {
		p.RetryPolicy = &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(sub.RetryPolicy.MinimumBackoff),
			MaximumBackoff: durationpb.New(sub.RetryPolicy.MaximumBackoff),
		}
	}
	if sub.PushConfig != nil {
		p.PushConfig = &pubsubpb.PushConfig{
			PushEndpoint: sub.PushConfig.Endpoint,
			Attributes:   sub.PushConfig.Attributes,
		}
	}
	return p
}

func snapshotToProto(snap *domain.Snapshot) *pubsubpb.Snapshot {
	p := &pubsubpb.Snapshot{
		Name:         snap.Name,
		Topic:        snap.TopicName,
		Labels:       snap.Labels,
	}
	if !snap.ExpireTime.IsZero() {
		p.ExpireTime = timestamppb.New(snap.ExpireTime)
	}
	return p
}

func pendingToProto(pm *domain.PendingMessage) *pubsubpb.ReceivedMessage {
	return &pubsubpb.ReceivedMessage{
		AckId:           pm.AckID,
		DeliveryAttempt: pm.DeliveryAttempt,
		Message: &pubsubpb.PubsubMessage{
			MessageId:   pm.Message.ID,
			Data:        pm.Message.Data,
			Attributes:  pm.Message.Attributes,
			OrderingKey: pm.Message.OrderingKey,
			PublishTime: timestamppb.New(pm.Message.PublishTime),
		},
	}
}
