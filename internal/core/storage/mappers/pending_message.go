package mappers

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
)

func PendingMessageModelToEntity(pm *models.PendingMessage) (*entities.PendingMessage, error) {
	msg := new(entities.Message)

	if err := msg.SetID(pm.ID); err != nil {
		return nil, err
	}
	if err := msg.SetData(pm.Data); err != nil {
		return nil, err
	}
	if err := msg.SetAttributes(pm.Attributes); err != nil {
		return nil, err
	}
	if err := msg.SetOrderingKey(pm.OrderingKey); err != nil {
		return nil, err
	}
	if err := msg.SetPublishTime(pm.PublishTime); err != nil {
		return nil, err
	}

	entity := new(entities.PendingMessage)
	if err := entity.SetMessage(msg); err != nil {
		return nil, err
	}
	if err := entity.SetAckID(pm.AckID); err != nil {
		return nil, err
	}
	if err := entity.SetDeliveryAttempt(pm.DeliveryAttempt); err != nil {
		return nil, err
	}
	if err := entity.SetAckDeadline(pm.AckDeadline); err != nil {
		return nil, err
	}

	return entity, nil
}

func PendingMessageEntityToModel(pm *entities.PendingMessage) (*models.PendingMessage, error) {
	model := new(models.PendingMessage)

	model.ID = pm.Message().ID()
	model.Data = pm.Message().Data()
	model.Attributes = pm.Message().Attributes()
	model.OrderingKey = pm.Message().OrderingKey()
	model.PublishTime = pm.Message().PublishTime()
	model.AckID = pm.AckID()
	model.AckDeadline = pm.AckDeadline()
	model.DeliveryAttempt = pm.DeliveryAttempt()

	return model, nil
}
