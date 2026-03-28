package repositories

import (
	"context"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/mappers"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type MessageRepository struct {
	state *memory.State
}

func NewMessageRepository(state *memory.State) *MessageRepository {
	return &MessageRepository{state: state}
}

func (r *MessageRepository) StoreMessage(_ context.Context, key types.FQDN, msg *entities.Message) error {
	model, err := mappers.MessageEntityToModel(msg)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map message", err)
	}
	r.state.Messages.Store(key, model)
	return nil
}

func (r *MessageRepository) GetMessage(_ context.Context, key types.FQDN) (*entities.Message, error) {
	v, ok := r.state.Messages.Load(key)
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "message not found")
	}
	entity, err := mappers.MessageModelToEntity(v.(*models.Message))
	if err != nil {
		return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map message", err)
	}
	return entity, nil
}

func (r *MessageRepository) DeleteMessage(_ context.Context, key types.FQDN) error {
	if _, ok := r.state.Messages.Load(key); !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "message not found")
	}
	r.state.Messages.Delete(key)
	return nil
}

func (r *MessageRepository) ListMessagesByTopic(_ context.Context, topicName types.FQDN) ([]*entities.Message, error) {
	prefix := topicName + "/messages/"

	var raw []*models.Message
	r.state.Messages.Range(func(k, v any) bool {
		key := k.(types.FQDN)
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			raw = append(raw, v.(*models.Message))
		}
		return true
	})

	out := make([]*entities.Message, len(raw))
	for i, m := range raw {
		var err error
		out[i], err = mappers.MessageModelToEntity(m)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map message", err)
		}
	}
	return out, nil
}
