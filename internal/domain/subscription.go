package domain

import "time"

// DeliveryType determines how messages are delivered to subscribers.
type DeliveryType int

const (
	DeliveryPull DeliveryType = iota
	DeliveryPush
)

// PushConfig holds push delivery configuration.
type PushConfig struct {
	Endpoint   string
	Attributes map[string]string
}

// DeadLetterPolicy configures dead-letter handling.
type DeadLetterPolicy struct {
	DeadLetterTopic     string
	MaxDeliveryAttempts int32
}

// RetryPolicy configures retry behaviour.
type RetryPolicy struct {
	MinimumBackoff time.Duration
	MaximumBackoff time.Duration
}

// Subscription represents a PubSub subscription.
type Subscription struct {
	Name                     string
	TopicName                string
	Labels                   map[string]string
	PushConfig               *PushConfig
	AckDeadline              time.Duration
	RetainAckedMessages      bool
	MessageRetention         time.Duration
	Filter                   string
	DeadLetterPolicy         *DeadLetterPolicy
	RetryPolicy              *RetryPolicy
	EnableMessageOrdering    bool
	EnableExactlyOnceDelivery bool
	CreatedAt                time.Time
}
