package repositories

import (
	"context"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/mappers"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type TopicRepository struct {
	state *memory.State
}

func NewTopicRepository(state *memory.State) *TopicRepository {
	return &TopicRepository{state: state}
}

func (r *TopicRepository) CreateTopic(_ context.Context, topic *entities.Topic) (*entities.Topic, error) {
	model, err := mappers.TopicEntityToModel(topic)
	if err != nil {
		return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
	}

	loadedTopic, loaded := r.state.Topics.LoadOrStore(topic.Name(), model)

	if loaded {
		return nil, types.NewPersistenceError(types.PersistenceAlreadyExists, "topic already exists")
	}

	if loadedTopic, ok := loadedTopic.(*models.Topic); ok {
		return mappers.TopicModelToEntity(loadedTopic)
	}

	return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", nil)
}

func (r *TopicRepository) GetTopic(_ context.Context, name types.FQDN) (*entities.Topic, error) {
	v, ok := r.state.Topics.Load(name)
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "topic not found")
	}
	entity, err := mappers.TopicModelToEntity(v.(*models.Topic))
	if err != nil {
		return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
	}
	return entity, nil
}

func (r *TopicRepository) UpdateTopic(_ context.Context, topic *entities.Topic) error {
	model, err := mappers.TopicEntityToModel(topic)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
	}
	if _, ok := r.state.Topics.Load(topic.Name()); !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "topic not found")
	}
	r.state.Topics.Store(topic.Name(), model)
	return nil
}

func (r *TopicRepository) DeleteTopic(_ context.Context, name types.FQDN) error {
	if _, ok := r.state.Topics.Load(name); !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "topic not found")
	}
	r.state.Topics.Delete(name)
	return nil
}

func (r *TopicRepository) ListTopics(_ context.Context, project string) ([]*entities.Topic, error) {
	prefix := types.FQDN("projects/" + project + "/topics/")
	if !prefix.IsValid() {
		return nil, types.NewPersistenceError(types.PersistencePreconditionFailed, "invalid project name")
	}

	var raw []*models.Topic
	r.state.Topics.Range(func(k, v any) bool {
		fqdn := k.(types.FQDN)
		if len(fqdn) >= len(prefix) && fqdn[:len(prefix)] == prefix {
			raw = append(raw, v.(*models.Topic))
		}
		return true
	})

	out := make([]*entities.Topic, len(raw))
	for i, t := range raw {
		var err error
		out[i], err = mappers.TopicModelToEntity(t)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
		}
	}
	return out, nil
}
