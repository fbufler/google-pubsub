package usecases

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type SubscriberProvider interface {
	Topics() *repositories.TopicRepository
	Subscriptions() *repositories.SubscriptionRepository
	PendingMessages() *repositories.PendingMessageRepository
	Messages() *repositories.MessageRepository
}

type SubscriberUsecase struct {
	uow UnitOfWork[SubscriberProvider]
}

func NewSubscriber(uow UnitOfWork[SubscriberProvider]) *SubscriberUsecase {
	return &SubscriberUsecase{uow: uow}
}

func (s *SubscriberUsecase) CreateSubscription(sub *entities.Subscription) error {
	_ = sub.SetCreatedAt(time.Now())
	return s.uow.Do(func(p SubscriberProvider) error {
		if _, err := p.Topics().GetTopic(sub.TopicName()); err != nil {
			return fromPersistence(err)
		}
		if err := p.Subscriptions().CreateSubscription(sub); err != nil {
			return fromPersistence(err)
		}
		p.PendingMessages().InitSubscription(sub.Name())
		return nil
	})
}

func (s *SubscriberUsecase) GetSubscription(name types.FQDN) (*entities.Subscription, error) {
	var sub *entities.Subscription
	err := s.uow.Do(func(p SubscriberProvider) error {
		var err error
		sub, err = p.Subscriptions().GetSubscription(name)
		if err != nil {
			return fromPersistence(err)
		}
		return nil
	})
	return sub, err
}

func (s *SubscriberUsecase) UpdateSubscription(sub *entities.Subscription) error {
	return s.uow.Do(func(p SubscriberProvider) error {
		if err := p.Subscriptions().UpdateSubscription(sub); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
}

func (s *SubscriberUsecase) DeleteSubscription(name types.FQDN) error {
	return s.uow.Do(func(p SubscriberProvider) error {
		if err := p.Subscriptions().DeleteSubscription(name); err != nil {
			return fromPersistence(err)
		}
		p.PendingMessages().DropSubscription(name)
		return nil
	})
}

func (s *SubscriberUsecase) ListSubscriptions(project string) ([]*entities.Subscription, error) {
	var subs []*entities.Subscription
	err := s.uow.Do(func(p SubscriberProvider) error {
		var err error
		subs, err = p.Subscriptions().ListSubscriptions(project)
		if err != nil {
			return fromPersistence(err)
		}
		return nil
	})
	return subs, err
}

func (s *SubscriberUsecase) ModifyPushConfig(subName types.FQDN, config *types.PushConfig) error {
	return s.uow.Do(func(p SubscriberProvider) error {
		sub, err := p.Subscriptions().GetSubscription(subName)
		if err != nil {
			return fromPersistence(err)
		}
		_ = sub.SetPushConfig(config)
		if err := p.Subscriptions().UpdateSubscription(sub); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
}

// Pull returns up to maxMessages available pending messages for subName,
// updating their ack deadlines and delivery attempts atomically.
func (s *SubscriberUsecase) Pull(subName types.FQDN, maxMessages int32) ([]*entities.PendingMessage, error) {
	var result []*entities.PendingMessage
	err := s.uow.Do(func(p SubscriberProvider) error {
		sub, err := p.Subscriptions().GetSubscription(subName)
		if err != nil {
			return fromPersistence(err)
		}

		pending, err := p.PendingMessages().ListPending(subName)
		if err != nil {
			return fromPersistence(err)
		}

		now := time.Now()

		// For ordered subscriptions: track which ordering keys already have an
		// outstanding (leased) message so we never deliver more than one at a time.
		leasedKeys := make(map[string]bool)
		if sub.EnableMessageOrdering() {
			for _, pm := range pending {
				if pm.Message().OrderingKey() != "" && pm.AckDeadline().After(now) {
					leasedKeys[pm.Message().OrderingKey()] = true
				}
			}
		}

		for _, pm := range pending {
			if int32(len(result)) >= maxMessages {
				break
			}
			if pm.AckDeadline().After(now) {
				continue // already leased
			}
			if sub.EnableMessageOrdering() && pm.Message().OrderingKey() != "" {
				if leasedKeys[pm.Message().OrderingKey()] {
					continue // wait for outstanding message on this key
				}
				leasedKeys[pm.Message().OrderingKey()] = true
			}
			_ = pm.SetAckDeadline(now.Add(sub.AckDeadline()))
			_ = pm.SetDeliveryAttempt(pm.DeliveryAttempt() + 1)
			result = append(result, pm)
		}

		// Persist the updated deadlines/attempt counts back to state.
		if err := p.PendingMessages().ReplacePending(subName, pending); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
	return result, err
}

func (s *SubscriberUsecase) Acknowledge(subName types.FQDN, ackIDs []string) error {
	return s.uow.Do(func(p SubscriberProvider) error {
		if err := p.PendingMessages().RemoveByAckID(subName, ackIDs); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
}

func (s *SubscriberUsecase) ModifyAckDeadline(subName types.FQDN, ackIDs []string, deadline time.Duration) error {
	return s.uow.Do(func(p SubscriberProvider) error {
		pending, err := p.PendingMessages().ListPending(subName)
		if err != nil {
			return fromPersistence(err)
		}

		set := make(map[string]bool, len(ackIDs))
		for _, id := range ackIDs {
			set[id] = true
		}
		now := time.Now()
		for _, pm := range pending {
			if set[pm.AckID()] {
				_ = pm.SetAckDeadline(now.Add(deadline))
			}
		}

		if err := p.PendingMessages().ReplacePending(subName, pending); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
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
		// Best-effort publish to DLQ; ignore errors if topic doesn't exist.
		_ = s.uow.Do(func(p SubscriberProvider) error {
			now := time.Now()
			for _, m := range dlqMsgs {
				id := newMsgID()
				_ = m.SetID(id)
				_ = m.SetPublishTime(now)
				key := types.FQDN(dlqTopic.String() + "/messages/" + id)
				_ = p.Messages().StoreMessage(key, m)
			}

			dlqSubs, err := p.Subscriptions().ListSubscriptionsByTopic(dlqTopic)
			if err != nil {
				return nil
			}
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
				_ = p.PendingMessages().AppendPending(dlqSub.Name(), pendingMsgs)
			}
			return nil
		})
		_ = s.Acknowledge(subName, dlqAckIDs)
	}

	return keep, nil
}
