package usecases

import (
	"context"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type SnapshotUsecase struct {
	snapshots       *repositories.SnapshotRepository
	subscriptions   *repositories.SubscriptionRepository
	messages        *repositories.MessageRepository
	pendingMessages *repositories.PendingMessageRepository
}

func NewSnapshotUsecase(
	snapshots *repositories.SnapshotRepository,
	subscriptions *repositories.SubscriptionRepository,
	messages *repositories.MessageRepository,
	pendingMessages *repositories.PendingMessageRepository,
) *SnapshotUsecase {
	return &SnapshotUsecase{
		snapshots:       snapshots,
		subscriptions:   subscriptions,
		messages:        messages,
		pendingMessages: pendingMessages,
	}
}

// CreateSnapshot captures the current unacked message state of a subscription.
func (s *SnapshotUsecase) CreateSnapshot(ctx context.Context, snap *entities.Snapshot) error {
	sub, err := s.subscriptions.GetSubscription(ctx, snap.SubscriptionName())
	if err != nil {
		return fromPersistence(err)
	}
	if err := snap.SetTopicName(sub.TopicName()); err != nil {
		return types.WrapUsecaseError(types.UsecaseInternal, "failed to set snapshot topic", err)
	}
	if err := snap.SetExpireTime(time.Now().Add(7 * 24 * time.Hour)); err != nil {
		return types.WrapUsecaseError(types.UsecaseInternal, "failed to set snapshot expire time", err)
	}
	snap.SetCreatedAt(time.Now())

	pending, err := s.pendingMessages.ListAll(ctx, snap.SubscriptionName())
	if err != nil {
		return fromPersistence(err)
	}
	unackedMsgIDs := make([]string, 0, len(pending))
	for _, pm := range pending {
		unackedMsgIDs = append(unackedMsgIDs, pm.Message().ID())
	}
	snap.SetUnackedMsgIDs(unackedMsgIDs)

	if err := s.snapshots.CreateSnapshot(ctx, snap); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SnapshotUsecase) GetSnapshot(ctx context.Context, name types.FQDN) (*entities.Snapshot, error) {
	snap, err := s.snapshots.GetSnapshot(ctx, name)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return snap, nil
}

func (s *SnapshotUsecase) UpdateSnapshot(ctx context.Context, snap *entities.Snapshot) error {
	if err := s.snapshots.UpdateSnapshot(ctx, snap); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SnapshotUsecase) DeleteSnapshot(ctx context.Context, name types.FQDN) error {
	if err := s.snapshots.DeleteSnapshot(ctx, name); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SnapshotUsecase) ListSnapshots(ctx context.Context, project string) ([]*entities.Snapshot, error) {
	snaps, err := s.snapshots.ListSnapshots(ctx, project)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return snaps, nil
}

// SeekToTime replaces the subscription's pending queue with all messages
// published at or after time t.
func (s *SnapshotUsecase) SeekToTime(ctx context.Context, subName types.FQDN, t time.Time) error {
	sub, err := s.subscriptions.GetSubscription(ctx, subName)
	if err != nil {
		return fromPersistence(err)
	}
	msgs, err := s.messages.ListMessagesByTopic(ctx, sub.TopicName())
	if err != nil {
		return fromPersistence(err)
	}
	var newPending []*entities.PendingMessage
	for _, m := range msgs {
		if !m.PublishTime().Before(t) {
			pm, err := newPendingMessage(m, subName)
			if err != nil {
				return err
			}
			newPending = append(newPending, pm)
		}
	}
	if err := s.pendingMessages.ReplaceAll(ctx, subName, newPending); err != nil {
		return fromPersistence(err)
	}
	return nil
}

// SeekToSnapshot replaces the subscription's pending queue with the messages
// that were unacked at the time the snapshot was created.
func (s *SnapshotUsecase) SeekToSnapshot(ctx context.Context, subName types.FQDN, snapName types.FQDN) error {
	snap, err := s.snapshots.GetSnapshot(ctx, snapName)
	if err != nil {
		return fromPersistence(err)
	}
	sub, err := s.subscriptions.GetSubscription(ctx, subName)
	if err != nil {
		return fromPersistence(err)
	}
	msgs, err := s.messages.ListMessagesByTopic(ctx, sub.TopicName())
	if err != nil {
		return fromPersistence(err)
	}
	unackedSet := make(map[string]bool, len(snap.UnackedMsgIDs()))
	for _, id := range snap.UnackedMsgIDs() {
		unackedSet[id] = true
	}
	snapTime := snap.CreatedAt()
	var newPending []*entities.PendingMessage
	for _, m := range msgs {
		if unackedSet[m.ID()] || !m.PublishTime().Before(snapTime) {
			pm, err := newPendingMessage(m, subName)
			if err != nil {
				return err
			}
			newPending = append(newPending, pm)
		}
	}
	if err := s.pendingMessages.ReplaceAll(ctx, subName, newPending); err != nil {
		return fromPersistence(err)
	}
	return nil
}

// newPendingMessage creates a PendingMessage for the given message and subscription.
func newPendingMessage(m *entities.Message, subName types.FQDN) (*entities.PendingMessage, error) {
	pm := new(entities.PendingMessage)
	if err := pm.SetMessage(m); err != nil {
		return nil, types.WrapUsecaseError(types.UsecaseInternal, "set pending message", err)
	}
	if err := pm.SetAckID(newAckID()); err != nil {
		return nil, types.WrapUsecaseError(types.UsecaseInternal, "set ack id", err)
	}
	if err := pm.SetAckDeadline(time.Time{}); err != nil {
		return nil, types.WrapUsecaseError(types.UsecaseInternal, "set ack deadline", err)
	}
	if err := pm.SetSubscription(subName); err != nil {
		return nil, types.WrapUsecaseError(types.UsecaseInternal, "set subscription", err)
	}
	return pm, nil
}
