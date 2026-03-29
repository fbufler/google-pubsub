package memory

import (
	"sync"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/storage/models"
)

const QueueSize = 10_000

// SubscriptionQueue is the per-subscription message queue.
// Undelivered messages live in the buffered channel; leased messages live in inFlight.
type SubscriptionQueue struct {
	ch       chan *models.PendingMessage
	inFlight sync.Map // ackID (string) → *models.PendingMessage
	notify   chan struct{}
}

func NewSubscriptionQueue() *SubscriptionQueue {
	return &SubscriptionQueue{
		ch:     make(chan *models.PendingMessage, QueueSize),
		notify: make(chan struct{}, 1),
	}
}

// Enqueue adds a message to the pending channel.
// Returns false if the channel is full (queue capacity exhausted).
func (q *SubscriptionQueue) Enqueue(pm *models.PendingMessage) bool {
	select {
	case q.ch <- pm:
	default:
		return false
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

// Notify returns the channel that is signalled whenever a message is enqueued.
func (q *SubscriptionQueue) Notify() <-chan struct{} {
	return q.notify
}

// TryDequeue removes and returns one pending message, or nil if the channel is empty.
func (q *SubscriptionQueue) TryDequeue() *models.PendingMessage {
	select {
	case pm := <-q.ch:
		return pm
	default:
		return nil
	}
}

// StoreInFlight records a message as leased.
func (q *SubscriptionQueue) StoreInFlight(pm *models.PendingMessage) {
	q.inFlight.Store(pm.AckID, pm)
}

// DeleteInFlight removes a leased message.
func (q *SubscriptionQueue) DeleteInFlight(ackID string) {
	q.inFlight.Delete(ackID)
}

// LoadInFlight returns a leased message by ack ID.
func (q *SubscriptionQueue) LoadInFlight(ackID string) (*models.PendingMessage, bool) {
	v, ok := q.inFlight.Load(ackID)
	if !ok {
		return nil, false
	}
	return v.(*models.PendingMessage), true
}

// ListInFlight returns all currently leased messages.
func (q *SubscriptionQueue) ListInFlight() []*models.PendingMessage {
	var out []*models.PendingMessage
	q.inFlight.Range(func(_, v any) bool {
		out = append(out, v.(*models.PendingMessage))
		return true
	})
	return out
}

// ClearInFlight removes all currently leased messages from the in-flight map.
func (q *SubscriptionQueue) ClearInFlight() {
	q.inFlight.Range(func(key, _ any) bool {
		q.inFlight.Delete(key)
		return true
	})
}

// RequeueExpired moves any in-flight messages whose ack deadline has passed back to the channel.
func (q *SubscriptionQueue) RequeueExpired() {
	now := time.Now()
	q.inFlight.Range(func(key, v any) bool {
		pm := v.(*models.PendingMessage)
		if !pm.AckDeadline.IsZero() && pm.AckDeadline.Before(now) {
			q.inFlight.Delete(key)
			pm.AckDeadline = time.Time{}
			q.Enqueue(pm)
		}
		return true
	})
}
