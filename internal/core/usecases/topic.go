package usecases

import (
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

func (t *TopicUsecase) CreateTopic(topic *entities.Topic) error {
	_ = topic.SetCreatedAt(time.Now())
	if err := t.topics.CreateTopic(topic); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (t *TopicUsecase) GetTopic(name types.FQDN) (*entities.Topic, error) {
	topic, err := t.topics.GetTopic(name)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return topic, nil
}

func (t *TopicUsecase) UpdateTopic(topic *entities.Topic) error {
	if err := t.topics.UpdateTopic(topic); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (t *TopicUsecase) DeleteTopic(name types.FQDN) error {
	if err := t.topics.DeleteTopic(name); err != nil {
		return fromPersistence(err)
	}
	return nil
}

func (t *TopicUsecase) ListTopics(project string) ([]*entities.Topic, error) {
	topics, err := t.topics.ListTopics(project)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return topics, nil
}

func (t *TopicUsecase) ListTopicSubscriptions(topicName types.FQDN) ([]types.FQDN, error) {
	if _, err := t.topics.GetTopic(topicName); err != nil {
		return nil, fromPersistence(err)
	}
	subs, err := t.subscriptions.ListSubscriptionsByTopic(topicName)
	if err != nil {
		return nil, fromPersistence(err)
	}
	names := make([]types.FQDN, len(subs))
	for i, sub := range subs {
		names[i] = sub.Name()
	}
	return names, nil
}

func (t *TopicUsecase) ListTopicSnapshots(topicName types.FQDN) ([]*entities.Snapshot, error) {
	snaps, err := t.snapshots.ListSnapshotsByTopic(topicName)
	if err != nil {
		return nil, fromPersistence(err)
	}
	return snaps, nil
}

func (t *TopicUsecase) DetachSubscription(subName types.FQDN) error {
	sub, err := t.subscriptions.GetSubscription(subName)
	if err != nil {
		return fromPersistence(err)
	}
	sub.DetachTopic()
	if err := t.subscriptions.UpdateSubscription(sub); err != nil {
		return fromPersistence(err)
	}
	return nil
}
