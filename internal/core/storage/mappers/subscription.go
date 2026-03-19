package mappers

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
)

func SubscriptionModelToEntity(subscription *models.Subscription) (*entities.Subscription, error) {
	entity := new(entities.Subscription)

	if err := entity.SetName(subscription.Name); err != nil {
		return nil, err
	}

	if err := entity.SetTopicName(subscription.TopicName); err != nil {
		return nil, err
	}

	if err := entity.SetLabels(subscription.Labels); err != nil {
		return nil, err
	}

	if err := entity.SetPushConfig(subscription.PushConfig); err != nil {
		return nil, err
	}

	if err := entity.SetAckDeadline(subscription.AckDeadline); err != nil {
		return nil, err
	}

	if err := entity.SetRetainAckedMessages(subscription.RetainAckedMessages); err != nil {
		return nil, err
	}

	if err := entity.SetMessageRetention(subscription.MessageRetention); err != nil {
		return nil, err
	}

	if err := entity.SetFilter(subscription.Filter); err != nil {
		return nil, err
	}

	if subscription.DeadLetterPolicy != nil {
		if err := entity.SetDeadLetterPolicy(subscription.DeadLetterPolicy); err != nil {
			return nil, err
		}
	}

	if subscription.RetryPolicy != nil {
		if err := entity.SetRetryPolicy(subscription.RetryPolicy); err != nil {
			return nil, err
		}
	}

	if err := entity.SetEnableMessageOrdering(subscription.EnableMessageOrdering); err != nil {
		return nil, err
	}

	if err := entity.SetEnableExactlyOnceDelivery(subscription.EnableExactlyOnceDelivery); err != nil {
		return nil, err
	}

	if err := entity.SetCreatedAt(subscription.CreatedAt); err != nil {
		return nil, err
	}

	return entity, nil
}

func SubscriptionEntityToModel(subscription *entities.Subscription) (*models.Subscription, error) {
	model := new(models.Subscription)

	model.Name = subscription.Name()
	model.TopicName = subscription.TopicName()
	model.Labels = subscription.Labels()
	model.PushConfig = subscription.PushConfig()
	model.AckDeadline = subscription.AckDeadline()
	model.RetainAckedMessages = subscription.RetainAckedMessages()
	model.MessageRetention = subscription.MessageRetention()
	model.Filter = subscription.Filter()
	model.DeadLetterPolicy = subscription.DeadLetterPolicy()
	model.RetryPolicy = subscription.RetryPolicy()
	model.EnableMessageOrdering = subscription.EnableMessageOrdering()
	model.EnableExactlyOnceDelivery = subscription.EnableExactlyOnceDelivery()
	model.CreatedAt = subscription.CreatedAt()

	return model, nil
}
