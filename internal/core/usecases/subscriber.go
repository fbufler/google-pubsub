package usecases

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type SubscriberUsecase struct {
	topics          *repositories.TopicRepository
	subscriptions   *repositories.SubscriptionRepository
	pendingMessages *repositories.PendingMessageRepository
	messages        *repositories.MessageRepository
}

func NewSubscriber(
	topics *repositories.TopicRepository,
	subscriptions *repositories.SubscriptionRepository,
	pendingMessages *repositories.PendingMessageRepository,
	messages *repositories.MessageRepository,
) *SubscriberUsecase {
	return &SubscriberUsecase{
		topics:          topics,
		subscriptions:   subscriptions,
		pendingMessages: pendingMessages,
		messages:        messages,
	}
}

func (s *SubscriberUsecase) CreateSubscription(sub *entities.Subscription) error {
	_ = sub.SetCreatedAt(time.Now())
	if _, err := s.topics.GetTopic(sub.TopicName()); err != nil {
		return fromPersistence(err)
	}
	if err := s.subscriptions.CreateSubscription(sub); err != nil {
		return fromPersistence(err)
	}
	s.pendingMessages.InitSubscription(sub.Name())
	return nil
}

func (s *SubscriberUsecase) GetSubscription(name types.FQDN) (*entities.Subscription, error) {
	sub, err := s.subscriptions.GetSubscription(name)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return sub, nil
}

func (s *SubscriberUsecase) UpdateSubscription(sub *entities.Subscription) error {
	if err := s.subscriptions.UpdateSubscription(sub); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SubscriberUsecase) DeleteSubscription(name types.FQDN) error {
	if err := s.subscriptions.DeleteSubscription(name); err != nil {
		return fromPersistence(err)
	}
	s.pendingMessages.DropSubscription(name)
	return nil
}

func (s *SubscriberUsecase) ListSubscriptions(project string) ([]*entities.Subscription, error) {
	subs, err := s.subscriptions.ListSubscriptions(project)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return subs, nil
}

func (s *SubscriberUsecase) ModifyPushConfig(subName types.FQDN, config *types.PushConfig) error {
	sub, err := s.subscriptions.GetSubscription(subName)
	if err != nil {
		return fromPersistence(err)
	}
	_ = sub.SetPushConfig(config)
	if err := s.subscriptions.UpdateSubscription(sub); err != nil {
		return fromPersistence(err)
	}
	return nil
}

// Pull returns up to maxMessages pending messages for subName.
func (s *SubscriberUsecase) Pull(subName types.FQDN, maxMessages int32) ([]*entities.PendingMessage, error) {
	sub, err := s.subscriptions.GetSubscription(subName)
	if err != nil {
		return nil, fromPersistence(err)
	}
	result, err := s.pendingMessages.Pull(subName, int(maxMessages), sub.AckDeadline(), sub.EnableMessageOrdering())
	if err != nil {
		return nil, fromPersistence(err)
	}
	return result, nil
}

func (s *SubscriberUsecase) Acknowledge(subName types.FQDN, ackIDs []string) error {
	if err := s.pendingMessages.AcknowledgeByAckID(subName, ackIDs); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (s *SubscriberUsecase) ModifyAckDeadline(subName types.FQDN, ackIDs []string, deadline time.Duration) error {
	if err := s.pendingMessages.UpdateDeadline(subName, ackIDs, deadline); err != nil {
		return fromPersistence(err)
	}
	return nil
}

// HandleDeadLetters filters msgs that have exceeded the subscription's max delivery
// attempts, forwards them to the dead-letter topic, and acknowledges them.
// Returns only the messages that should still be delivered.
func (s *SubscriberUsecase) HandleDeadLetters(subName types.FQDN, msgs []*entities.PendingMessage) ([]*entities.PendingMessage, error) {
	sub, err := s.GetSubscription(subName)
	if err != nil || sub.DeadLetterPolicy() == nil {
		return msgs, nil
	}
	max := sub.DeadLetterPolicy().MaxDeliveryAttempts
	dlqTopic := types.FQDN(sub.DeadLetterPolicy().DeadLetterTopic)

	var keep []*entities.PendingMessage
	var dlqMsgs []*entities.Message
	var dlqAckIDs []string

	for _, pm := range msgs {
		if max > 0 && pm.DeliveryAttempt() >= max {
			dlqMsgs = append(dlqMsgs, pm.Message())
			dlqAckIDs = append(dlqAckIDs, pm.AckID())
		} else {
			keep = append(keep, pm)
		}
	}

	if len(dlqMsgs) > 0 {
		now := time.Now()
		for _, m := range dlqMsgs {
			id := newMsgID()
			_ = m.SetID(id)
			_ = m.SetPublishTime(now)
			key := types.FQDN(dlqTopic.String() + "/messages/" + id)
			_ = s.messages.StoreMessage(key, m)
		}

		dlqSubs, err := s.subscriptions.ListSubscriptionsByTopic(dlqTopic)
		if err == nil {
			for _, dlqSub := range dlqSubs {
				pendingMsgs := make([]*entities.PendingMessage, 0, len(dlqMsgs))
				for _, m := range dlqMsgs {
					pm := new(entities.PendingMessage)
					_ = pm.SetMessage(m)
					_ = pm.SetAckID(newAckID())
					_ = pm.SetAckDeadline(time.Time{})
					_ = pm.SetSubscription(dlqSub.Name())
					pendingMsgs = append(pendingMsgs, pm)
				}
				_ = s.pendingMessages.Enqueue(dlqSub.Name(), pendingMsgs)
			}
		}
		_ = s.Acknowledge(subName, dlqAckIDs)
	}

	return keep, nil
}
