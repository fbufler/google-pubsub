package domain

import "time"

// Topic represents a PubSub topic.
type Topic struct {
	Name              string
	Labels            map[string]string
	MessageRetention  time.Duration
	KmsKeyName        string
	CreatedAt         time.Time
}
