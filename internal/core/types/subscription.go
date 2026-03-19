package types

import (
	"time"
)

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
