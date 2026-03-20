package repositories

import (
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

func (r *TopicRepository) CreateTopic(topic *entities.Topic) error {
	model, err := mappers.TopicEntityToModel(topic)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
	}
	if _, ok := r.state.Topics[topic.Name()]; ok {
		return types.NewPersistenceError(types.PersistenceAlreadyExists, "topic already exists")
	}
	r.state.Topics[topic.Name()] = model
	return nil
}

func (r *TopicRepository) GetTopic(name types.FQDN) (*entities.Topic, error) {
	topic, ok := r.state.Topics[name]
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "topic not found")
	}
	entity, err := mappers.TopicModelToEntity(topic)
	if err != nil {
		return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
	}
	return entity, nil
}

func (r *TopicRepository) UpdateTopic(topic *entities.Topic) error {
	model, err := mappers.TopicEntityToModel(topic)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
	}
	if _, ok := r.state.Topics[topic.Name()]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "topic not found")
	}
	r.state.Topics[topic.Name()] = model
	return nil
}

func (r *TopicRepository) DeleteTopic(name types.FQDN) error {
	if _, ok := r.state.Topics[name]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "topic not found")
	}
	delete(r.state.Topics, name)
	return nil
}

func (r *TopicRepository) ListTopics(project string) ([]*entities.Topic, error) {
	prefix := types.FQDN("projects/" + project + "/topics/")
	if !prefix.IsValid() {
		return nil, types.NewPersistenceError(types.PersistencePreconditionFailed, "invalid project name")
	}

	var out []*models.Topic
	for _, t := range r.state.Topics {
		if len(t.Name) >= len(prefix) && t.Name[:len(prefix)] == prefix {
			out = append(out, t)
		}
	}

	topics := make([]*entities.Topic, len(out))
	for i, t := range out {
		var err error
		topics[i], err = mappers.TopicModelToEntity(t)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map topic", err)
		}
	}
	return topics, nil
}
