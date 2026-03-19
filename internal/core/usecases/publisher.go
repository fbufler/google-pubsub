package usecases

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type PublishProvider interface {
	Topics() *repositories.TopicRepository
	Messages() *repositories.MessageRepository
	Subscriptions() *repositories.SubscriptionRepository
	PendingMessages() *repositories.PendingMessageRepository
}

type PublisherUsecase struct {
	uow UnitOfWork[PublishProvider]
}

func NewPublisher(uow UnitOfWork[PublishProvider]) *PublisherUsecase {
	return &PublisherUsecase{uow: uow}
}

// Publish assigns IDs and publish timestamps to msgs, stores them under topicName,
// and fans them out to all subscriptions on that topic.
// Returns the assigned message IDs in the same order as msgs.
func (pub *PublisherUsecase) Publish(topicName types.FQDN, msgs []*entities.Message) ([]string, error) {
	var ids []string
	err := pub.uow.Do(func(p PublishProvider) error {
		now := time.Now()
		ids = make([]string, 0, len(msgs))

		if _, err := p.Topics().GetTopic(topicName); err != nil {
			return err
		}

		for _, m := range msgs {
			id := newMsgID()
			_ = m.SetID(id)
			_ = m.SetPublishTime(now)
			ids = append(ids, id)

			key := types.FQDN(topicName.String() + "/messages/" + id)
			if err := p.Messages().StoreMessage(key, m); err != nil {
				return err
			}
		}

		subs, err := p.Subscriptions().ListSubscriptionsByTopic(topicName)
		if err != nil {
			return err
		}

		for _, sub := range subs {
			pendingMsgs := make([]*entities.PendingMessage, 0, len(msgs))
			for _, m := range msgs {
				pm := new(entities.PendingMessage)
				_ = pm.SetMessage(m)
				_ = pm.SetAckID(newAckID())
				_ = pm.SetAckDeadline(time.Time{}) // zero = available immediately
				_ = pm.SetSubscription(sub.Name())
				pendingMsgs = append(pendingMsgs, pm)
			}
			if err := p.PendingMessages().AppendPending(sub.Name(), pendingMsgs); err != nil {
				return err
			}
		}

		return nil
	})
	return ids, err
}
