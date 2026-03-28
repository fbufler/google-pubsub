package repositories

import (
	"context"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/mappers"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type PendingMessageRepository struct {
	state *memory.State
}

func NewPendingMessageRepository(state *memory.State) *PendingMessageRepository {
	return &PendingMessageRepository{state: state}
}

func (r *PendingMessageRepository) queue(subName types.FQDN) (*memory.SubscriptionQueue, bool) {
	v, ok := r.state.Queues.Load(subName)
	if !ok {
		return nil, false
	}
	return v.(*memory.SubscriptionQueue), true
}

// InitSubscription creates an empty queue for a subscription.
func (r *PendingMessageRepository) InitSubscription(_ context.Context, subName types.FQDN) {
	r.state.Queues.Store(subName, memory.NewSubscriptionQueue())
}

// DropSubscription removes the queue for a subscription.
func (r *PendingMessageRepository) DropSubscription(_ context.Context, subName types.FQDN) {
	r.state.Queues.Delete(subName)
}

// Enqueue adds pending messages to a subscription's channel.
func (r *PendingMessageRepository) Enqueue(_ context.Context, subName types.FQDN, pms []*entities.PendingMessage) error {
	q, ok := r.queue(subName)
	if !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	for _, pm := range pms {
		model, err := mappers.PendingMessageEntityToModel(pm)
		if err != nil {
			return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map pending message", err)
		}
		q.Enqueue(model)
	}
	return nil
}

// Pull returns up to max unleased messages, respecting ordering keys when ordered is true.
// Expired leases are requeued first; returned messages are moved to in-flight with the given deadline.
func (r *PendingMessageRepository) Pull(_ context.Context, subName types.FQDN, max int, ackDeadline time.Duration, ordered bool) ([]*entities.PendingMessage, error) {
	q, ok := r.queue(subName)
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}

	q.RequeueExpired()

	deadline := time.Now().Add(ackDeadline)

	// Collect ordering keys already in-flight to enforce per-key ordering.
	leasedKeys := map[string]bool{}
	if ordered {
		for _, pm := range q.ListInFlight() {
			if pm.OrderingKey != "" {
				leasedKeys[pm.OrderingKey] = true
			}
		}
	}

	var result []*entities.PendingMessage
	var skipped []*models.PendingMessage

	for len(result) < max {
		pm := q.TryDequeue()
		if pm == nil {
			break
		}
		if ordered && pm.OrderingKey != "" && leasedKeys[pm.OrderingKey] {
			skipped = append(skipped, pm)
			continue
		}
		if ordered && pm.OrderingKey != "" {
			leasedKeys[pm.OrderingKey] = true
		}

		pm.AckDeadline = deadline
		pm.DeliveryAttempt++
		q.StoreInFlight(pm)

		entity, err := mappers.PendingMessageModelToEntity(pm)
		if err != nil {
			// Re-enqueue on mapping failure to avoid message loss.
			pm.AckDeadline = time.Time{}
			q.DeleteInFlight(pm.AckID)
			q.Enqueue(pm)
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map pending message", err)
		}
		result = append(result, entity)
	}

	// Return skipped ordering-key messages to the channel.
	for _, pm := range skipped {
		q.Enqueue(pm)
	}

	return result, nil
}

// ListAll returns every pending message — both queued (channel) and in-flight.
// The channel is drained and immediately re-enqueued, so this must be called
// while the UoW mutex is held to prevent interleaving with concurrent publishes.
func (r *PendingMessageRepository) ListAll(_ context.Context, subName types.FQDN) ([]*entities.PendingMessage, error) {
	q, ok := r.queue(subName)
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}

	var queued []*models.PendingMessage
	for {
		pm := q.TryDequeue()
		if pm == nil {
			break
		}
		queued = append(queued, pm)
	}
	for _, pm := range queued {
		q.Enqueue(pm)
	}

	all := append(queued, q.ListInFlight()...)
	out := make([]*entities.PendingMessage, 0, len(all))
	for _, m := range all {
		pm, err := mappers.PendingMessageModelToEntity(m)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map pending message", err)
		}
		out = append(out, pm)
	}
	return out, nil
}

// ReplaceAll drains the channel, clears in-flight, and enqueues pms as the new queue.
// Used by Seek operations to atomically reset a subscription's message set.
func (r *PendingMessageRepository) ReplaceAll(_ context.Context, subName types.FQDN, pms []*entities.PendingMessage) error {
	q, ok := r.queue(subName)
	if !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	for q.TryDequeue() != nil {
	}
	q.ClearInFlight()
	for _, pm := range pms {
		model, err := mappers.PendingMessageEntityToModel(pm)
		if err != nil {
			return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map pending message", err)
		}
		q.Enqueue(model)
	}
	return nil
}

// AcknowledgeByAckID removes the given messages from in-flight.
func (r *PendingMessageRepository) AcknowledgeByAckID(_ context.Context, subName types.FQDN, ackIDs []string) error {
	q, ok := r.queue(subName)
	if !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	for _, id := range ackIDs {
		q.DeleteInFlight(id)
	}
	return nil
}

// UpdateDeadline extends (or nacks with zero) the ack deadline for in-flight messages.
func (r *PendingMessageRepository) UpdateDeadline(_ context.Context, subName types.FQDN, ackIDs []string, deadline time.Duration) error {
	q, ok := r.queue(subName)
	if !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	newDeadline := time.Now().Add(deadline)
	for _, id := range ackIDs {
		if pm, found := q.LoadInFlight(id); found {
			pm.AckDeadline = newDeadline
		}
	}
	return nil
}
