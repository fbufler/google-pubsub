package handler

import (
	"context"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

var _ pubsubpbconnect.SubscriberHandler = (*Subscriber)(nil)

// Subscriber implements the PubSub Subscriber gRPC service by delegating each
// method to an independently injectable handler function.
type Subscriber struct {
	pubsubpbconnect.UnimplementedSubscriberHandler
	createSubscription  func(context.Context, *connect.Request[pubsubpb.Subscription]) (*connect.Response[pubsubpb.Subscription], error)
	getSubscription     func(context.Context, *connect.Request[pubsubpb.GetSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error)
	updateSubscription  func(context.Context, *connect.Request[pubsubpb.UpdateSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error)
	deleteSubscription  func(context.Context, *connect.Request[pubsubpb.DeleteSubscriptionRequest]) (*connect.Response[emptypb.Empty], error)
	listSubscriptions   func(context.Context, *connect.Request[pubsubpb.ListSubscriptionsRequest]) (*connect.Response[pubsubpb.ListSubscriptionsResponse], error)
	pull                func(context.Context, *connect.Request[pubsubpb.PullRequest]) (*connect.Response[pubsubpb.PullResponse], error)
	acknowledge         func(context.Context, *connect.Request[pubsubpb.AcknowledgeRequest]) (*connect.Response[emptypb.Empty], error)
	modifyAckDeadline   func(context.Context, *connect.Request[pubsubpb.ModifyAckDeadlineRequest]) (*connect.Response[emptypb.Empty], error)
	modifyPushConfig    func(context.Context, *connect.Request[pubsubpb.ModifyPushConfigRequest]) (*connect.Response[emptypb.Empty], error)
	streamingPull       func(context.Context, *connect.BidiStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error
	createSnapshot      func(context.Context, *connect.Request[pubsubpb.CreateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error)
	getSnapshot         func(context.Context, *connect.Request[pubsubpb.GetSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error)
	updateSnapshot      func(context.Context, *connect.Request[pubsubpb.UpdateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error)
	deleteSnapshot      func(context.Context, *connect.Request[pubsubpb.DeleteSnapshotRequest]) (*connect.Response[emptypb.Empty], error)
	listSnapshots       func(context.Context, *connect.Request[pubsubpb.ListSnapshotsRequest]) (*connect.Response[pubsubpb.ListSnapshotsResponse], error)
	seek                func(context.Context, *connect.Request[pubsubpb.SeekRequest]) (*connect.Response[pubsubpb.SeekResponse], error)
}

func NewSubscriber(ctx context.Context, subUC *usecases.SubscriberUsecase, snapUC *usecases.SnapshotUsecase) *Subscriber {
	return &Subscriber{
		createSubscription: NewCreateSubscription(ctx, subUC),
		getSubscription:    NewGetSubscription(ctx, subUC),
		updateSubscription: NewUpdateSubscription(ctx, subUC),
		deleteSubscription: NewDeleteSubscription(ctx, subUC),
		listSubscriptions:  NewListSubscriptions(ctx, subUC),
		pull:               NewPull(ctx, subUC),
		acknowledge:        NewAcknowledge(ctx, subUC),
		modifyAckDeadline:  NewModifyAckDeadline(ctx, subUC),
		modifyPushConfig:   NewModifyPushConfig(ctx, subUC),
		streamingPull:      NewStreamingPull(ctx, subUC),
		createSnapshot:     NewCreateSnapshot(ctx, snapUC),
		getSnapshot:        NewGetSnapshot(ctx, snapUC),
		updateSnapshot:     NewUpdateSnapshot(ctx, snapUC),
		deleteSnapshot:     NewDeleteSnapshot(ctx, snapUC),
		listSnapshots:      NewListSnapshots(ctx, snapUC),
		seek:               NewSeek(ctx, snapUC),
	}
}

func (s *Subscriber) CreateSubscription(ctx context.Context, req *connect.Request[pubsubpb.Subscription]) (*connect.Response[pubsubpb.Subscription], error) {
	return s.createSubscription(ctx, req)
}

func (s *Subscriber) GetSubscription(ctx context.Context, req *connect.Request[pubsubpb.GetSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	return s.getSubscription(ctx, req)
}

func (s *Subscriber) UpdateSubscription(ctx context.Context, req *connect.Request[pubsubpb.UpdateSubscriptionRequest]) (*connect.Response[pubsubpb.Subscription], error) {
	return s.updateSubscription(ctx, req)
}

func (s *Subscriber) DeleteSubscription(ctx context.Context, req *connect.Request[pubsubpb.DeleteSubscriptionRequest]) (*connect.Response[emptypb.Empty], error) {
	return s.deleteSubscription(ctx, req)
}

func (s *Subscriber) ListSubscriptions(ctx context.Context, req *connect.Request[pubsubpb.ListSubscriptionsRequest]) (*connect.Response[pubsubpb.ListSubscriptionsResponse], error) {
	return s.listSubscriptions(ctx, req)
}

func (s *Subscriber) Pull(ctx context.Context, req *connect.Request[pubsubpb.PullRequest]) (*connect.Response[pubsubpb.PullResponse], error) {
	return s.pull(ctx, req)
}

func (s *Subscriber) Acknowledge(ctx context.Context, req *connect.Request[pubsubpb.AcknowledgeRequest]) (*connect.Response[emptypb.Empty], error) {
	return s.acknowledge(ctx, req)
}

func (s *Subscriber) ModifyAckDeadline(ctx context.Context, req *connect.Request[pubsubpb.ModifyAckDeadlineRequest]) (*connect.Response[emptypb.Empty], error) {
	return s.modifyAckDeadline(ctx, req)
}

func (s *Subscriber) ModifyPushConfig(ctx context.Context, req *connect.Request[pubsubpb.ModifyPushConfigRequest]) (*connect.Response[emptypb.Empty], error) {
	return s.modifyPushConfig(ctx, req)
}

func (s *Subscriber) StreamingPull(ctx context.Context, stream *connect.BidiStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error {
	return s.streamingPull(ctx, stream)
}

func (s *Subscriber) CreateSnapshot(ctx context.Context, req *connect.Request[pubsubpb.CreateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	return s.createSnapshot(ctx, req)
}

func (s *Subscriber) GetSnapshot(ctx context.Context, req *connect.Request[pubsubpb.GetSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	return s.getSnapshot(ctx, req)
}

func (s *Subscriber) UpdateSnapshot(ctx context.Context, req *connect.Request[pubsubpb.UpdateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	return s.updateSnapshot(ctx, req)
}

func (s *Subscriber) DeleteSnapshot(ctx context.Context, req *connect.Request[pubsubpb.DeleteSnapshotRequest]) (*connect.Response[emptypb.Empty], error) {
	return s.deleteSnapshot(ctx, req)
}

func (s *Subscriber) ListSnapshots(ctx context.Context, req *connect.Request[pubsubpb.ListSnapshotsRequest]) (*connect.Response[pubsubpb.ListSnapshotsResponse], error) {
	return s.listSnapshots(ctx, req)
}

func (s *Subscriber) Seek(ctx context.Context, req *connect.Request[pubsubpb.SeekRequest]) (*connect.Response[pubsubpb.SeekResponse], error) {
	return s.seek(ctx, req)
}
