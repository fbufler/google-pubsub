package repositories

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/mappers"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type SubscriptionRepository struct {
	state *memory.State
}

func NewSubscriptionRepository(state *memory.State) *SubscriptionRepository {
	return &SubscriptionRepository{state: state}
}

func (r *SubscriptionRepository) CreateSubscription(sub *entities.Subscription) error {
	model, err := mappers.SubscriptionEntityToModel(sub)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map subscription", err)
	}
	if _, ok := r.state.Subscriptions[sub.Name()]; ok {
		return types.NewPersistenceError(types.PersistenceAlreadyExists, "subscription already exists")
	}
	r.state.Subscriptions[sub.Name()] = model
	return nil
}

func (r *SubscriptionRepository) GetSubscription(name types.FQDN) (*entities.Subscription, error) {
	sub, ok := r.state.Subscriptions[name]
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "subscription not found")
	}
	entity, err := mappers.SubscriptionModelToEntity(sub)
	if err != nil {
		return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map subscription", err)
	}
	return entity, nil
}

func (r *SubscriptionRepository) UpdateSubscription(sub *entities.Subscription) error {
	model, err := mappers.SubscriptionEntityToModel(sub)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map subscription", err)
	}
	if _, ok := r.state.Subscriptions[sub.Name()]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription not found")
	}
	r.state.Subscriptions[sub.Name()] = model
	return nil
}

func (r *SubscriptionRepository) DeleteSubscription(name types.FQDN) error {
	if _, ok := r.state.Subscriptions[name]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription not found")
	}
	delete(r.state.Subscriptions, name)
	return nil
}

func (r *SubscriptionRepository) ListSubscriptions(project string) ([]*entities.Subscription, error) {
	prefix := types.FQDN("projects/" + project + "/subscriptions/")
	if !prefix.IsValid() {
		return nil, types.NewPersistenceError(types.PersistencePreconditionFailed, "invalid project name")
	}

	var out []*models.Subscription
	for _, sub := range r.state.Subscriptions {
		if len(sub.Name) >= len(prefix) && sub.Name[:len(prefix)] == prefix {
			out = append(out, sub)
		}
	}

	subs := make([]*entities.Subscription, len(out))
	for i, sub := range out {
		var err error
		subs[i], err = mappers.SubscriptionModelToEntity(sub)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map subscription", err)
		}
	}
	return subs, nil
}

func (r *SubscriptionRepository) ListSubscriptionsByTopic(topicName types.FQDN) ([]*entities.Subscription, error) {
	var out []*models.Subscription
	for _, sub := range r.state.Subscriptions {
		if sub.TopicName == topicName {
			out = append(out, sub)
		}
	}

	subs := make([]*entities.Subscription, len(out))
	for i, sub := range out {
		var err error
		subs[i], err = mappers.SubscriptionModelToEntity(sub)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map subscription", err)
		}
	}
	return subs, nil
}
