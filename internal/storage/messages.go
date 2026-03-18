package storage

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/domain"
)

// AppendMessages adds messages to a topic's backlog and fans them out to
// all current subscriptions as pending messages.
func (s *Store) AppendMessages(topicName string, msgs []*domain.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[topicName]; !ok {
		return domain.ErrNotFound
	}
	s.messages[topicName] = append(s.messages[topicName], msgs...)
	for _, sub := range s.subscriptions {
		if sub.TopicName != topicName {
			continue
		}
		for _, m := range msgs {
			s.pending[sub.Name] = append(s.pending[sub.Name], &domain.PendingMessage{
				Message:     m,
				AckID:       newAckID(),
				AckDeadline: time.Time{}, // zero = available immediately
				Subscription: sub.Name,
			})
		}
	}
	return nil
}

// PullPending returns up to maxMessages pending messages for a subscription,
// resetting their ack deadlines.
func (s *Store) PullPending(subName string, maxMessages int32) ([]*domain.PendingMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subscriptions[subName]
	if !ok {
		return nil, domain.ErrNotFound
	}
	pending := s.pending[subName]
	now := time.Now()
	var result []*domain.PendingMessage
	for _, p := range pending {
		if int32(len(result)) >= maxMessages {
			break
		}
		// Only deliver messages whose ack deadline has not been exceeded, or
		// that are newly available (deadline in the past means re-delivery).
		if p.AckDeadline.After(now) {
			continue // already leased, not yet expired
		}
		p.AckDeadline = now.Add(sub.AckDeadline)
		p.DeliveryAttempt++
		result = append(result, p)
	}
	return result, nil
}

// Acknowledge removes pending messages by their ack IDs.
func (s *Store) Acknowledge(subName string, ackIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[subName]; !ok {
		return domain.ErrNotFound
	}
	set := make(map[string]struct{}, len(ackIDs))
	for _, id := range ackIDs {
		set[id] = struct{}{}
	}
	filtered := s.pending[subName][:0]
	for _, p := range s.pending[subName] {
		if _, remove := set[p.AckID]; !remove {
			filtered = append(filtered, p)
		}
	}
	s.pending[subName] = filtered
	return nil
}

// ModifyAckDeadline extends or resets the ack deadline for the given ack IDs.
func (s *Store) ModifyAckDeadline(subName string, ackIDs []string, deadline time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[subName]; !ok {
		return domain.ErrNotFound
	}
	set := make(map[string]struct{}, len(ackIDs))
	for _, id := range ackIDs {
		set[id] = struct{}{}
	}
	for _, p := range s.pending[subName] {
		if _, ok := set[p.AckID]; ok {
			p.AckDeadline = time.Now().Add(deadline)
		}
	}
	return nil
}
