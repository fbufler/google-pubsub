package mappers

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
)

func SnapshotModelToEntity(snapshot *models.Snapshot) (*entities.Snapshot, error) {
	entity := new(entities.Snapshot)

	if err := entity.SetName(snapshot.Name); err != nil {
		return nil, err
	}

	if err := entity.SetSubscriptionName(snapshot.SubscriptionName); err != nil {
		return nil, err
	}

	if err := entity.SetTopicName(snapshot.TopicName); err != nil {
		return nil, err
	}

	if err := entity.SetLabels(snapshot.Labels); err != nil {
		return nil, err
	}

	if err := entity.RestoreExpireTime(snapshot.ExpireTime); err != nil {
		return nil, err
	}

	entity.SetCreatedAt(snapshot.CreatedAt)
	entity.SetUnackedMsgIDs(snapshot.UnackedMsgIDs)

	return entity, nil
}

func SnapshotEntityToModel(snapshot *entities.Snapshot) (*models.Snapshot, error) {
	model := new(models.Snapshot)

	model.Name = snapshot.Name()
	model.SubscriptionName = snapshot.SubscriptionName()
	model.TopicName = snapshot.TopicName()
	model.Labels = snapshot.Labels()
	model.ExpireTime = snapshot.ExpireTime()
	model.CreatedAt = snapshot.CreatedAt()
	model.UnackedMsgIDs = snapshot.UnackedMsgIDs()

	return model, nil
}
