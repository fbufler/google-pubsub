package entities

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// Topic represents a PubSub topic.
type Topic struct {
	name             types.FQDN
	labels           types.Labels
	messageRetention time.Duration
	kmsKeyName       types.CMEK
	createdAt        time.Time
}

// Name returns the topic name.
func (t *Topic) Name() types.FQDN {
	return t.name
}

// SetName sets the topic name.
func (t *Topic) SetName(name types.FQDN) error {
	if !name.IsValid() {
		return ErrInvalidTopicName
	}

	t.name = name
	return nil
}

// Labels returns the topic labels.
func (t *Topic) Labels() types.Labels {
	return t.labels
}

// SetLabels sets the topic labels.
func (t *Topic) SetLabels(labels types.Labels) error {
	if !labels.IsValid() {
		return ErrInvalidLabels
	}

	t.labels = labels
	return nil
}

// MessageRetention returns the topic message retention duration.
func (t *Topic) MessageRetention() time.Duration {
	return t.messageRetention
}

// SetMessageRetention sets the topic message retention duration.
// Constraints:
// - Minimum: 1 minute.
// - Maximum: 31 days.
func (t *Topic) SetMessageRetention(messageRetention time.Duration) error {
	if messageRetention < 1*time.Minute || messageRetention > 31*24*time.Hour {
		return ErrInvalidTopicMessageRetention
	}

	t.messageRetention = messageRetention
	return nil
}

// KmsKeyName returns the topic KMS key name.
func (t *Topic) KmsKeyName() types.CMEK {
	return t.kmsKeyName
}

// SetKmsKeyName sets the topic KMS key name.
func (t *Topic) SetKmsKeyName(kmsKeyName types.CMEK) error {
	if !kmsKeyName.IsValid() {
		return ErrInvalidTopicKmsKeyName
	}

	t.kmsKeyName = kmsKeyName
	return nil
}

// CreatedAt returns the topic creation time.
func (t *Topic) CreatedAt() time.Time {
	return t.createdAt
}

// SetCreatedAt sets the topic creation time.
func (t *Topic) SetCreatedAt(createdAt time.Time) error {
	t.createdAt = createdAt
	return nil
}
