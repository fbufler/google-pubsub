package models

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// Subscription represents a PubSub subscription.
type Subscription struct {
	Name                      types.FQDN
	TopicName                 types.FQDN
	Labels                    types.Labels
	PushConfig                *types.PushConfig
	AckDeadline               time.Duration
	RetainAckedMessages       bool
	MessageRetention          time.Duration
	Filter                    string
	DeadLetterPolicy          *types.DeadLetterPolicy
	RetryPolicy               *types.RetryPolicy
	EnableMessageOrdering     bool
	EnableExactlyOnceDelivery bool
	CreatedAt                 time.Time
}
