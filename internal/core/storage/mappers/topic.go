package mappers

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
)

func TopicModelToEntity(topic *models.Topic) (*entities.Topic, error) {
	entity := new(entities.Topic)

	if err := entity.SetName(topic.Name); err != nil {
		return nil, err
	}

	if err := entity.SetLabels(topic.Labels); err != nil {
		return nil, err
	}

	if topic.MessageRetention > 0 {
		if err := entity.SetMessageRetention(topic.MessageRetention); err != nil {
			return nil, err
		}
	}

	if topic.KmsKeyName != "" {
		if err := entity.SetKmsKeyName(topic.KmsKeyName); err != nil {
			return nil, err
		}
	}

	if err := entity.SetCreatedAt(topic.CreatedAt); err != nil {
		return nil, err
	}

	return entity, nil
}

func TopicEntityToModel(topic *entities.Topic) (*models.Topic, error) {
	model := new(models.Topic)

	model.Name = topic.Name()
	model.Labels = topic.Labels()
	model.MessageRetention = topic.MessageRetention()
	model.KmsKeyName = topic.KmsKeyName()
	model.CreatedAt = topic.CreatedAt()

	return model, nil
}
