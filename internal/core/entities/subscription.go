package entities

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// Subscription represents a PubSub subscription.
type Subscription struct {
	name                      types.FQDN
	topicName                 types.FQDN
	labels                    types.Labels
	pushConfig                *types.PushConfig
	ackDeadline               time.Duration
	retainAckedMessages       bool
	messageRetention          time.Duration
	filter                    string
	deadLetterPolicy          *types.DeadLetterPolicy
	retryPolicy               *types.RetryPolicy
	enableMessageOrdering     bool
	enableExactlyOnceDelivery bool
	createdAt                 time.Time
}

func (s *Subscription) Name() types.FQDN {
	return s.name
}

func (s *Subscription) SetName(name types.FQDN) error {
	if !name.IsValid() {
		return ErrInvalidSubscriptionName
	}

	s.name = name
	return nil
}

func (s *Subscription) TopicName() types.FQDN {
	return s.topicName
}

func (s *Subscription) SetTopicName(topicName types.FQDN) error {
	if !topicName.IsValid() {
		return ErrInvalidTopicName
	}

	s.topicName = topicName
	return nil
}

func (s *Subscription) Labels() types.Labels {
	return s.labels
}

func (s *Subscription) SetLabels(labels types.Labels) error {
	if !labels.IsValid() {
		return ErrInvalidLabels
	}

	s.labels = labels
	return nil
}

func (s *Subscription) PushConfig() *types.PushConfig {
	return s.pushConfig
}

func (s *Subscription) SetPushConfig(pushConfig *types.PushConfig) error {
	s.pushConfig = pushConfig
	return nil
}

func (s *Subscription) AckDeadline() time.Duration {
	return s.ackDeadline
}

// SetAckDeadline sets the ack deadline for the subscription.
//
// Constraints
// - Min: 10 seconds.
// - Max: 600 seconds (10 minutes).
// Note: If not specified, the default is 10 seconds.
func (s *Subscription) SetAckDeadline(ackDeadline time.Duration) error {
	if ackDeadline < 10*time.Second || ackDeadline > 10*time.Minute {
		return ErrInvalidSubscriptionAckDeadline
	}

	s.ackDeadline = ackDeadline
	return nil
}

func (s *Subscription) RetainAckedMessages() bool {
	return s.retainAckedMessages
}

// SetRetainAckedMessages sets whether to retain acked messages.
func (s *Subscription) SetRetainAckedMessages(retainAckedMessages bool) error {
	s.retainAckedMessages = retainAckedMessages
	return nil
}

func (s *Subscription) MessageRetention() time.Duration {
	return s.messageRetention
}

// SetMessageRetention sets the message retention duration for the subscription.
// Constraints:
// - Min: 10 minutes.
// - Max: 7 days.
// Note: If retainAckedMessages is true, this determines how long those acked messages stay "seekable."
func (s *Subscription) SetMessageRetention(messageRetention time.Duration) error {
	if messageRetention < 10*time.Minute || messageRetention > 7*24*time.Hour {
		return ErrInvalidSubscriptionMessageRetention
	}

	s.messageRetention = messageRetention
	return nil
}

func (s *Subscription) Filter() string {
	return s.filter
}

// SetFilter sets the filter expression for the subscription.
// Constraints:
// - Length: Max 256 characters.
func (s *Subscription) SetFilter(filter string) error {
	if len(filter) > 256 {
		return ErrInvalidSubscriptionFilter
	}

	s.filter = filter
	return nil
}

func (s *Subscription) DeadLetterPolicy() *types.DeadLetterPolicy {
	return s.deadLetterPolicy
}

// SetDeadLetterPolicy sets the dead letter policy for the subscription.
// Constraints:
// - Max Delivery Attempts: Must be between 5 and 100.
// Topic: The dead letter topic must exist before the subscription is created/updated.
func (s *Subscription) SetDeadLetterPolicy(deadLetterPolicy *types.DeadLetterPolicy) error {
	if deadLetterPolicy.MaxDeliveryAttempts < 5 || deadLetterPolicy.MaxDeliveryAttempts > 100 {
		return ErrInvalidSubscriptionDeadLetterPolicy
	}

	s.deadLetterPolicy = deadLetterPolicy
	return nil
}

func (s *Subscription) RetryPolicy() *types.RetryPolicy {
	return s.retryPolicy
}

// SetRetryPolicy sets the retry policy for the subscription.
// Constraints:
// - Minimum Backoff: Min 0s, Max 600s (Default: 10s).
// - Maximum Backoff: Min 0s, Max 600s (Default: 600s).
// - Validation: Minimum Backoff must be less than or equal to Maximum Backoff.
func (s *Subscription) SetRetryPolicy(retryPolicy *types.RetryPolicy) error {
	if retryPolicy.MinimumBackoff < 0 || retryPolicy.MinimumBackoff > 600*time.Second {
		return ErrInvalidSubscriptionRetryPolicy
	}
	if retryPolicy.MaximumBackoff < 0 || retryPolicy.MaximumBackoff > 600*time.Second {
		return ErrInvalidSubscriptionRetryPolicy
	}
	if retryPolicy.MinimumBackoff > retryPolicy.MaximumBackoff {
		return ErrInvalidSubscriptionRetryPolicy
	}

	s.retryPolicy = retryPolicy
	return nil
}

func (s *Subscription) EnableMessageOrdering() bool {
	return s.enableMessageOrdering
}

func (s *Subscription) SetEnableMessageOrdering(enableMessageOrdering bool) error {
	s.enableMessageOrdering = enableMessageOrdering
	return nil
}

func (s *Subscription) EnableExactlyOnceDelivery() bool {
	return s.enableExactlyOnceDelivery
}

func (s *Subscription) SetEnableExactlyOnceDelivery(enableExactlyOnceDelivery bool) error {
	s.enableExactlyOnceDelivery = enableExactlyOnceDelivery
	return nil
}

// DetachTopic marks the subscription's topic as deleted using the PubSub sentinel value.
func (s *Subscription) DetachTopic() {
	s.topicName = "_deleted-topic_"
}

func (s *Subscription) CreatedAt() time.Time {
	return s.createdAt
}

func (s *Subscription) SetCreatedAt(createdAt time.Time) error {
	s.createdAt = createdAt
	return nil
}
