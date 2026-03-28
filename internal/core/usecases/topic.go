package usecases

import (
	"context"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type TopicUsecase struct {
	topics        *repositories.TopicRepository
	subscriptions *repositories.SubscriptionRepository
	snapshots     *repositories.SnapshotRepository
}

func NewTopicUsecase(
	topics *repositories.TopicRepository,
	subscriptions *repositories.SubscriptionRepository,
	snapshots *repositories.SnapshotRepository,
) *TopicUsecase {
	return &TopicUsecase{topics: topics, subscriptions: subscriptions, snapshots: snapshots}
}

func (t *TopicUsecase) CreateTopic(ctx context.Context, topic *entities.Topic) (*entities.Topic, error) {
	err := topic.SetCreatedAt(time.Now())
	if err != nil {
		return nil, types.NewUsecaseError(types.UsecaseInternal, "set created at")
	}

	topic, err = t.topics.CreateTopic(ctx, topic)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return topic, nil
}

func (t *TopicUsecase) GetTopic(ctx context.Context, name types.FQDN) (*entities.Topic, error) {
	topic, err := t.topics.GetTopic(ctx, name)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return topic, nil
}

func (t *TopicUsecase) UpdateTopic(ctx context.Context, topic *entities.Topic) error {
	if err := t.topics.UpdateTopic(ctx, topic); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (t *TopicUsecase) DeleteTopic(ctx context.Context, name types.FQDN) error {
	if err := t.topics.DeleteTopic(ctx, name); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (t *TopicUsecase) ListTopics(ctx context.Context, project string) ([]*entities.Topic, error) {
	topics, err := t.topics.ListTopics(ctx, project)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return topics, nil
}

func (t *TopicUsecase) ListTopicSubscriptions(ctx context.Context, topicName types.FQDN) ([]types.FQDN, error) {
	if _, err := t.topics.GetTopic(ctx, topicName); err != nil {
		return nil, fromPersistence(err)
	}
	subs, err := t.subscriptions.ListSubscriptionsByTopic(ctx, topicName)
	if err != nil {
		return nil, fromPersistence(err)
	}
	names := make([]types.FQDN, len(subs))
	for i, sub := range subs {
		names[i] = sub.Name()
	}
	return names, nil
}

func (t *TopicUsecase) ListTopicSnapshots(ctx context.Context, topicName types.FQDN) ([]*entities.Snapshot, error) {
	snaps, err := t.snapshots.ListSnapshotsByTopic(ctx, topicName)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return snaps, nil
}

func (t *TopicUsecase) DetachSubscription(ctx context.Context, subName types.FQDN) error {
	sub, err := t.subscriptions.GetSubscription(ctx, subName)
	if err != nil {
		return fromPersistence(err)
	}
	sub.DetachTopic()
	if err := t.subscriptions.UpdateSubscription(ctx, sub); err != nil {
		return fromPersistence(err)
	}
	return nil
}
