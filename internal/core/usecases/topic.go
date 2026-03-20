package usecases

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type TopicProvider interface {
	Topics() *repositories.TopicRepository
	Subscriptions() *repositories.SubscriptionRepository
	Snapshots() *repositories.SnapshotRepository
}

type TopicUsecase struct {
	uow UnitOfWork[TopicProvider]
}

func NewTopicUsecase(uow UnitOfWork[TopicProvider]) *TopicUsecase {
	return &TopicUsecase{uow: uow}
}

func (t *TopicUsecase) CreateTopic(topic *entities.Topic) error {
	_ = topic.SetCreatedAt(time.Now())
	return t.uow.Do(func(p TopicProvider) error {
		if err := p.Topics().CreateTopic(topic); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
}

func (t *TopicUsecase) GetTopic(name types.FQDN) (*entities.Topic, error) {
	var topic *entities.Topic
	err := t.uow.Do(func(p TopicProvider) error {
		var err error
		topic, err = p.Topics().GetTopic(name)
		if err != nil {
			return fromPersistence(err)
		}
		return nil
	})
	return topic, err
}

func (t *TopicUsecase) UpdateTopic(topic *entities.Topic) error {
	return t.uow.Do(func(p TopicProvider) error {
		if err := p.Topics().UpdateTopic(topic); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
}

func (t *TopicUsecase) DeleteTopic(name types.FQDN) error {
	return t.uow.Do(func(p TopicProvider) error {
		if err := p.Topics().DeleteTopic(name); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
}

func (t *TopicUsecase) ListTopics(project string) ([]*entities.Topic, error) {
	var topics []*entities.Topic
	err := t.uow.Do(func(p TopicProvider) error {
		var err error
		topics, err = p.Topics().ListTopics(project)
		if err != nil {
			return fromPersistence(err)
		}
		return nil
	})
	return topics, err
}

func (t *TopicUsecase) ListTopicSubscriptions(topicName types.FQDN) ([]types.FQDN, error) {
	var names []types.FQDN
	err := t.uow.Do(func(p TopicProvider) error {
		if _, err := p.Topics().GetTopic(topicName); err != nil {
			return fromPersistence(err)
		}
		subs, err := p.Subscriptions().ListSubscriptionsByTopic(topicName)
		if err != nil {
			return fromPersistence(err)
		}
		for _, sub := range subs {
			names = append(names, sub.Name())
		}
		return nil
	})
	return names, err
}

func (t *TopicUsecase) ListTopicSnapshots(topicName types.FQDN) ([]*entities.Snapshot, error) {
	var snaps []*entities.Snapshot
	err := t.uow.Do(func(p TopicProvider) error {
		var err error
		snaps, err = p.Snapshots().ListSnapshotsByTopic(topicName)
		if err != nil {
			return fromPersistence(err)
		}
		return nil
	})
	return snaps, err
}

func (t *TopicUsecase) DetachSubscription(subName types.FQDN) error {
	return t.uow.Do(func(p TopicProvider) error {
		sub, err := p.Subscriptions().GetSubscription(subName)
		if err != nil {
			return fromPersistence(err)
		}
		sub.DetachTopic()
		if err := p.Subscriptions().UpdateSubscription(sub); err != nil {
			return fromPersistence(err)
		}
		return nil
	})
}
