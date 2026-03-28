package usecases

import (
	"context"
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
func (pub *PublisherUsecase) Publish(ctx context.Context, topicName types.FQDN, msgs []*entities.Message) ([]string, error) {
	if _, err := pub.topics.GetTopic(ctx, topicName); err != nil {
		return nil, fromPersistence(err)
	}

	now := time.Now()
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		id := newMsgID()
		if err := m.SetID(id); err != nil {
			return nil, types.WrapUsecaseError(types.UsecaseInternal, "set message id", err)
		}
		if err := m.SetPublishTime(now); err != nil {
			return nil, types.WrapUsecaseError(types.UsecaseInternal, "set message publish time", err)
		}
		ids = append(ids, id)

		key := types.FQDN(topicName.String() + "/messages/" + id)
		if err := pub.messages.StoreMessage(ctx, key, m); err != nil {
			return nil, fromPersistence(err)
		}
	}

	subs, err := pub.subscriptions.ListSubscriptionsByTopic(ctx, topicName)
	if err != nil {
		return nil, fromPersistence(err)
	}

	for _, sub := range subs {
		pendingMsgs := make([]*entities.PendingMessage, 0, len(msgs))
		for _, m := range msgs {
			pm, err := newPendingMessage(m, sub.Name())
			if err != nil {
				return nil, err
			}
			pendingMsgs = append(pendingMsgs, pm)
		}
		if err := pub.pendingMessages.Enqueue(ctx, sub.Name(), pendingMsgs); err != nil {
			return nil, fromPersistence(err)
		}
	}

	return ids, nil
}
