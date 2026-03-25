package usecases

import (
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
func (s *SnapshotUsecase) CreateSnapshot(snap *entities.Snapshot) error {
	sub, err := s.subscriptions.GetSubscription(snap.SubscriptionName())
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

	pending, err := s.pendingMessages.ListAll(snap.SubscriptionName())
	if err != nil {
		return fromPersistence(err)
	}
	unackedMsgIDs := make([]string, 0, len(pending))
	for _, pm := range pending {
		unackedMsgIDs = append(unackedMsgIDs, pm.Message().ID())
	}
	snap.SetUnackedMsgIDs(unackedMsgIDs)

	if err := s.snapshots.CreateSnapshot(snap); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SnapshotUsecase) GetSnapshot(name types.FQDN) (*entities.Snapshot, error) {
	snap, err := s.snapshots.GetSnapshot(name)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return snap, nil
}

func (s *SnapshotUsecase) UpdateSnapshot(snap *entities.Snapshot) error {
	if err := s.snapshots.UpdateSnapshot(snap); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SnapshotUsecase) DeleteSnapshot(name types.FQDN) error {
	if err := s.snapshots.DeleteSnapshot(name); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SnapshotUsecase) ListSnapshots(project string) ([]*entities.Snapshot, error) {
	snaps, err := s.snapshots.ListSnapshots(project)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return snaps, nil
}

// SeekToTime replaces the subscription's pending queue with all messages
// published at or after time t.
func (s *SnapshotUsecase) SeekToTime(subName types.FQDN, t time.Time) error {
	sub, err := s.subscriptions.GetSubscription(subName)
	if err != nil {
		return fromPersistence(err)
	}
	msgs, err := s.messages.ListMessagesByTopic(sub.TopicName())
	if err != nil {
		return fromPersistence(err)
	}
	var newPending []*entities.PendingMessage
	for _, m := range msgs {
		if !m.PublishTime().Before(t) {
			pm := new(entities.PendingMessage)
			_ = pm.SetMessage(m)
			_ = pm.SetAckID(newAckID())
			_ = pm.SetAckDeadline(time.Time{})
			_ = pm.SetSubscription(subName)
			newPending = append(newPending, pm)
		}
	}
	if err := s.pendingMessages.ReplaceAll(subName, newPending); err != nil {
		return fromPersistence(err)
	}
	return nil
}

// SeekToSnapshot replaces the subscription's pending queue with the messages
// that were unacked at the time the snapshot was created.
func (s *SnapshotUsecase) SeekToSnapshot(subName types.FQDN, snapName types.FQDN) error {
	snap, err := s.snapshots.GetSnapshot(snapName)
	if err != nil {
		return fromPersistence(err)
	}
	sub, err := s.subscriptions.GetSubscription(subName)
	if err != nil {
		return fromPersistence(err)
	}
	msgs, err := s.messages.ListMessagesByTopic(sub.TopicName())
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
			pm := new(entities.PendingMessage)
			_ = pm.SetMessage(m)
			_ = pm.SetAckID(newAckID())
			_ = pm.SetAckDeadline(time.Time{})
			_ = pm.SetSubscription(subName)
			newPending = append(newPending, pm)
		}
	}
	if err := s.pendingMessages.ReplaceAll(subName, newPending); err != nil {
		return fromPersistence(err)
	}
	return nil
}
