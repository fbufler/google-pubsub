package models

import (
	"time"
)

type Message struct {
	ID          string
	Data        []byte
	Attributes  map[string]string
	OrderingKey string
	PublishTime time.Time
}
