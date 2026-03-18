package domain

import "time"

// Message represents a PubSub message stored in a topic.
type Message struct {
	ID          string
	Data        []byte
	Attributes  map[string]string
	OrderingKey string
	PublishTime time.Time
}

// PendingMessage is a message awaiting acknowledgement on a subscription.
type PendingMessage struct {
	Message          *Message
	AckID            string
	DeliveryAttempt  int32
	AckDeadline      time.Time
	Subscription     string
}
