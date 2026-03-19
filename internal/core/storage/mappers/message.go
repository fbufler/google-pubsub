package mappers

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
)

func MessageModelToEntity(message *models.Message) (*entities.Message, error) {
	entity := new(entities.Message)

	if err := entity.SetID(message.ID); err != nil {
		return nil, err
	}

	if err := entity.SetData(message.Data); err != nil {
		return nil, err
	}

	if err := entity.SetAttributes(message.Attributes); err != nil {
		return nil, err
	}

	if err := entity.SetOrderingKey(message.OrderingKey); err != nil {
		return nil, err
	}

	if err := entity.SetPublishTime(message.PublishTime); err != nil {
		return nil, err
	}

	return entity, nil
}

func MessageEntityToModel(message *entities.Message) (*models.Message, error) {
	model := new(models.Message)

	model.ID = message.ID()
	model.Data = message.Data()
	model.Attributes = message.Attributes()
	model.OrderingKey = message.OrderingKey()
	model.PublishTime = message.PublishTime()

	return model, nil
}
