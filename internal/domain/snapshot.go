package domain

import "time"

// Snapshot represents a point-in-time snapshot of a subscription.
type Snapshot struct {
	Name             string
	SubscriptionName string
	TopicName        string
	Labels           map[string]string
	ExpireTime       time.Time
	CreatedAt        time.Time
	// UnackedMsgIDs holds the message IDs that were unacked at snapshot time.
	UnackedMsgIDs    []string
}
