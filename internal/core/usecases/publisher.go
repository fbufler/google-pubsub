package usecases

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type PublisherUsecase struct {
	topics          *repositories.TopicRepository
	messages        *repositories.MessageRepository
	subscriptions   *repositories.SubscriptionRepository
	pendingMessages *repositories.PendingMessageRepository
}

func NewPublisher(
	topics *repositories.TopicRepository,
	messages *repositories.MessageRepository,
	subscriptions *repositories.SubscriptionRepository,
	pendingMessages *repositories.PendingMessageRepository,
) *PublisherUsecase {
	return &PublisherUsecase{
		topics:          topics,
		messages:        messages,
		subscriptions:   subscriptions,
		pendingMessages: pendingMessages,
	}
}

// Publish assigns IDs and publish timestamps to msgs, stores them under topicName,
// and fans them out to all subscriptions on that topic.
// Returns the assigned message IDs in the same order as msgs.
func (pub *PublisherUsecase) Publish(topicName types.FQDN, msgs []*entities.Message) ([]string, error) {
	if _, err := pub.topics.GetTopic(topicName); err != nil {
		return nil, fromPersistence(err)
	}

	now := time.Now()
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		id := newMsgID()
		_ = m.SetID(id)
		_ = m.SetPublishTime(now)
		ids = append(ids, id)

		key := types.FQDN(topicName.String() + "/messages/" + id)
		if err := pub.messages.StoreMessage(key, m); err != nil {
			return nil, fromPersistence(err)
		}
	}

	subs, err := pub.subscriptions.ListSubscriptionsByTopic(topicName)
	if err != nil {
		return nil, fromPersistence(err)
	}

	for _, sub := range subs {
		pendingMsgs := make([]*entities.PendingMessage, 0, len(msgs))
		for _, m := range msgs {
			pm := new(entities.PendingMessage)
			_ = pm.SetMessage(m)
			_ = pm.SetAckID(newAckID())
			_ = pm.SetAckDeadline(time.Time{})
			_ = pm.SetSubscription(sub.Name())
			pendingMsgs = append(pendingMsgs, pm)
		}
		if err := pub.pendingMessages.Enqueue(sub.Name(), pendingMsgs); err != nil {
			return nil, fromPersistence(err)
		}
	}

	return ids, nil
}
