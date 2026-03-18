package storage

import (
	"github.com/fbufler/google-pubsub/internal/domain"
)

func (s *Store) CreateSubscription(sub *domain.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[sub.Name]; ok {
		return domain.ErrAlreadyExists
	}
	s.subscriptions[sub.Name] = sub
	s.pending[sub.Name] = nil
	return nil
}

func (s *Store) GetSubscription(name string) (*domain.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subscriptions[name]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return sub, nil
}

func (s *Store) UpdateSubscription(sub *domain.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[sub.Name]; !ok {
		return domain.ErrNotFound
	}
	s.subscriptions[sub.Name] = sub
	return nil
}

func (s *Store) DeleteSubscription(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[name]; !ok {
		return domain.ErrNotFound
	}
	delete(s.subscriptions, name)
	delete(s.pending, name)
	return nil
}

func (s *Store) ListSubscriptions(project string) ([]*domain.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := "projects/" + project + "/subscriptions/"
	out := make([]*domain.Subscription, 0)
	for _, sub := range s.subscriptions {
		if len(sub.Name) >= len(prefix) && sub.Name[:len(prefix)] == prefix {
			out = append(out, sub)
		}
	}
	return out, nil
}
