package models

import "time"

type PendingMessage struct {
	ID              string
	Data            []byte
	Attributes      map[string]string
	OrderingKey     string
	PublishTime     time.Time
	AckID           string
	AckDeadline     time.Time
	DeliveryAttempt int32
}
