package entities

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// Snapshot represents a point-in-time snapshot of a subscription.
type Snapshot struct {
	name             types.FQDN
	subscriptionName types.FQDN
	topicName        types.FQDN
	labels           types.Labels
	expireTime       time.Time
	createdAt        time.Time
	unackedMsgIDs    []string
}

func (s *Snapshot) Name() types.FQDN {
	return s.name

}
func (s *Snapshot) SetName(name types.FQDN) error {
	if !name.IsValid() {
		return ErrInvalidSnapshotName
	}

	s.name = name
	return nil
}

func (s *Snapshot) SubscriptionName() types.FQDN {
	return s.subscriptionName
}

func (s *Snapshot) SetSubscriptionName(name types.FQDN) error {
	if !name.IsValid() {
		return ErrInvalidSubscriptionName
	}

	s.subscriptionName = name
	return nil
}

func (s *Snapshot) TopicName() types.FQDN {
	return s.topicName
}

func (s *Snapshot) SetTopicName(name types.FQDN) error {
	if !name.IsValid() {
		return ErrInvalidTopicName
	}

	s.topicName = name
	return nil
}

func (s *Snapshot) Labels() types.Labels {
	return s.labels
}

func (s *Snapshot) SetLabels(labels types.Labels) error {
	if !labels.IsValid() {
		return ErrInvalidLabels
	}

	s.labels = labels
	return nil
}

func (s *Snapshot) ExpireTime() time.Time {
	return s.expireTime
}

// SetExpireTime returns the time at which the snapshot will expire.
// Constraints:
// - must be in the future
// - must be at max 7 days from now
func (s *Snapshot) SetExpireTime(expireTime time.Time) error {
	if expireTime.Before(time.Now()) {
		return ErrSnapshotExpireTimeInPast
	}

	if time.Until(expireTime) > 7*24*time.Hour {
		return ErrSnapshotExpireTimeTooFar
	}

	s.expireTime = expireTime
	return nil
}

// RestoreExpireTime restores the expire time of the snapshot to the given time.
func (s *Snapshot) RestoreExpireTime(expireTime time.Time) error {
	s.expireTime = expireTime
	return nil
}

func (s *Snapshot) CreatedAt() time.Time {
	return s.createdAt
}

func (s *Snapshot) SetCreatedAt(createdAt time.Time) {
	s.createdAt = createdAt
}

func (s *Snapshot) UnackedMsgIDs() []string {
	return s.unackedMsgIDs
}

func (s *Snapshot) SetUnackedMsgIDs(unackedMsgIDs []string) {
	s.unackedMsgIDs = unackedMsgIDs
}
