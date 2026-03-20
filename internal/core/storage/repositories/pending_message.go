package repositories

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/mappers"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	storagemodels "github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type PendingMessageRepository struct {
	state *memory.State
}

func NewPendingMessageRepository(state *memory.State) *PendingMessageRepository {
	return &PendingMessageRepository{state: state}
}

// InitSubscription creates an empty pending queue for a subscription.
func (r *PendingMessageRepository) InitSubscription(subName types.FQDN) {
	r.state.PendingMessages[subName] = nil
}

// DropSubscription removes the pending queue for a subscription.
func (r *PendingMessageRepository) DropSubscription(subName types.FQDN) {
	delete(r.state.PendingMessages, subName)
}

// AppendPending adds pending messages to a subscription's queue.
func (r *PendingMessageRepository) AppendPending(subName types.FQDN, pms []*entities.PendingMessage) error {
	if _, ok := r.state.PendingMessages[subName]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	for _, pm := range pms {
		model, err := mappers.PendingMessageEntityToModel(pm)
		if err != nil {
			return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map pending message", err)
		}
		r.state.PendingMessages[subName] = append(r.state.PendingMessages[subName], model)
	}
	return nil
}

// ListPending returns all pending messages for a subscription.
func (r *PendingMessageRepository) ListPending(subName types.FQDN) ([]*entities.PendingMessage, error) {
	stored, ok := r.state.PendingMessages[subName]
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	out := make([]*entities.PendingMessage, len(stored))
	for i, model := range stored {
		pm, err := mappers.PendingMessageModelToEntity(model)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map pending message", err)
		}
		out[i] = pm
	}
	return out, nil
}

// ReplacePending atomically replaces all pending messages for a subscription.
func (r *PendingMessageRepository) ReplacePending(subName types.FQDN, pms []*entities.PendingMessage) error {
	if _, ok := r.state.PendingMessages[subName]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	stored := make([]*storagemodels.PendingMessage, 0, len(pms))
	for _, pm := range pms {
		model, err := mappers.PendingMessageEntityToModel(pm)
		if err != nil {
			return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map pending message", err)
		}
		stored = append(stored, model)
	}
	r.state.PendingMessages[subName] = stored
	return nil
}

// RemoveByAckID removes pending messages matching the given ack IDs.
func (r *PendingMessageRepository) RemoveByAckID(subName types.FQDN, ackIDs []string) error {
	if _, ok := r.state.PendingMessages[subName]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription queue not found")
	}
	set := make(map[string]struct{}, len(ackIDs))
	for _, id := range ackIDs {
		set[id] = struct{}{}
	}
	filtered := r.state.PendingMessages[subName][:0]
	for _, p := range r.state.PendingMessages[subName] {
		if _, remove := set[p.AckID]; !remove {
			filtered = append(filtered, p)
		}
	}
	r.state.PendingMessages[subName] = filtered
	return nil
}
