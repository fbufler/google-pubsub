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
	if _, loaded := r.state.Subscriptions.LoadOrStore(sub.Name(), model); loaded {
		return types.NewPersistenceError(types.PersistenceAlreadyExists, "subscription already exists")
	}
	return nil
}

func (r *SubscriptionRepository) GetSubscription(name types.FQDN) (*entities.Subscription, error) {
	v, ok := r.state.Subscriptions.Load(name)
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "subscription not found")
	}
	entity, err := mappers.SubscriptionModelToEntity(v.(*models.Subscription))
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
	if _, ok := r.state.Subscriptions.Load(sub.Name()); !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription not found")
	}
	r.state.Subscriptions.Store(sub.Name(), model)
	return nil
}

func (r *SubscriptionRepository) DeleteSubscription(name types.FQDN) error {
	if _, ok := r.state.Subscriptions.Load(name); !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "subscription not found")
	}
	r.state.Subscriptions.Delete(name)
	return nil
}

func (r *SubscriptionRepository) ListSubscriptions(project string) ([]*entities.Subscription, error) {
	prefix := types.FQDN("projects/" + project + "/subscriptions/")
	if !prefix.IsValid() {
		return nil, types.NewPersistenceError(types.PersistencePreconditionFailed, "invalid project name")
	}

	var raw []*models.Subscription
	r.state.Subscriptions.Range(func(k, v any) bool {
		fqdn := k.(types.FQDN)
		if len(fqdn) >= len(prefix) && fqdn[:len(prefix)] == prefix {
			raw = append(raw, v.(*models.Subscription))
		}
		return true
	})

	out := make([]*entities.Subscription, len(raw))
	for i, sub := range raw {
		var err error
		out[i], err = mappers.SubscriptionModelToEntity(sub)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map subscription", err)
		}
	}
	return out, nil
}

func (r *SubscriptionRepository) ListSubscriptionsByTopic(topicName types.FQDN) ([]*entities.Subscription, error) {
	var raw []*models.Subscription
	r.state.Subscriptions.Range(func(_, v any) bool {
		sub := v.(*models.Subscription)
		if sub.TopicName == topicName {
			raw = append(raw, sub)
		}
		return true
	})

	out := make([]*entities.Subscription, len(raw))
	for i, sub := range raw {
		var err error
		out[i], err = mappers.SubscriptionModelToEntity(sub)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map subscription", err)
		}
	}
	return out, nil
}
