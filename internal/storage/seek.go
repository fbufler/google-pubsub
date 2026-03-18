package storage

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/domain"
)

// CreateSnapshotForSub creates a snapshot capturing the current unacked message state of a subscription.
func (s *Store) CreateSnapshotForSub(snap *domain.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.snapshots[snap.Name]; ok {
		return domain.ErrAlreadyExists
	}
	sub, ok := s.subscriptions[snap.SubscriptionName]
	if !ok {
		return domain.ErrNotFound
	}
	snap.TopicName = sub.TopicName

	// Record the unacked message IDs at this point in time.
	pending := s.pending[snap.SubscriptionName]
	snap.UnackedMsgIDs = make([]string, 0, len(pending))
	for _, p := range pending {
		snap.UnackedMsgIDs = append(snap.UnackedMsgIDs, p.Message.ID)
	}
	// Also record the publish time of the last acked message (all messages up to now minus pending).
	snap.CreatedAt = time.Now()

	s.snapshots[snap.Name] = snap
	s.labels[snap.Name] = snap.Labels
	return nil
}

// SeekToSnapshot rewinds a subscription to the state captured by a snapshot.
func (s *Store) SeekToSnapshot(subName, snapName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, ok := s.subscriptions[subName]
	if !ok {
		return domain.ErrNotFound
	}
	snap, ok := s.snapshots[snapName]
	if !ok {
		return domain.ErrNotFound
	}

	unacked := make(map[string]bool, len(snap.UnackedMsgIDs))
	for _, id := range snap.UnackedMsgIDs {
		unacked[id] = true
	}

	// Messages published after the snapshot creation time are always redeliverable.
	allMsgs := s.messages[sub.TopicName]
	s.pending[subName] = nil
	for _, m := range allMsgs {
		if unacked[m.ID] || m.PublishTime.After(snap.CreatedAt) {
			s.pending[subName] = append(s.pending[subName], &domain.PendingMessage{
				Message:     m,
				AckID:       newAckID(),
				AckDeadline: time.Time{},
				Subscription: subName,
			})
		}
	}
	return nil
}

// SeekToTime rewinds a subscription to a point in time: messages published
// after t are re-queued for delivery.
func (s *Store) SeekToTime(subName string, t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, ok := s.subscriptions[subName]
	if !ok {
		return domain.ErrNotFound
	}

	allMsgs := s.messages[sub.TopicName]
	s.pending[subName] = nil
	for _, m := range allMsgs {
		if m.PublishTime.After(t) {
			s.pending[subName] = append(s.pending[subName], &domain.PendingMessage{
				Message:     m,
				AckID:       newAckID(),
				AckDeadline: time.Time{},
				Subscription: subName,
			})
		}
	}
	return nil
}
