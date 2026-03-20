package models

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// Snapshot represents a point-in-time snapshot of a subscription.
type Snapshot struct {
	Name             types.FQDN
	SubscriptionName types.FQDN
	TopicName        types.FQDN
	Labels           types.Labels
	ExpireTime       time.Time
	CreatedAt        time.Time
	UnackedMsgIDs    []string
}
