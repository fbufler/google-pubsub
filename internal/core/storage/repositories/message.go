package repositories

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/mappers"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type MessageRepository struct {
	state *memory.State
}

func NewMessageRepository(state *memory.State) *MessageRepository {
	return &MessageRepository{
		state: state,
	}
}

func (r *MessageRepository) StoreMessage(key types.FQDN, msg *entities.Message) error {
	model, err := mappers.MessageEntityToModel(msg)
	if err != nil {
		return err
	}

	r.state.Messages[key] = model

	return nil
}

func (r *MessageRepository) GetMessage(key types.FQDN) (*entities.Message, error) {
	model, ok := r.state.Messages[key]
	if !ok {
		return nil, ErrNotFound
	}

	return mappers.MessageModelToEntity(model)
}

func (r *MessageRepository) DeleteMessage(key types.FQDN) error {
	if _, ok := r.state.Messages[key]; !ok {
		return ErrNotFound
	}
	delete(r.state.Messages, key)

	return nil
}

func (r *MessageRepository) ListMessagesByTopic(topicName types.FQDN) ([]*entities.Message, error) {
	prefix := topicName + "/messages/"

	var out []*entities.Message
	for key, model := range r.state.Messages {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			msg, err := mappers.MessageModelToEntity(model)
			if err != nil {
				return nil, err
			}
			out = append(out, msg)
		}
	}

	return out, nil
}
