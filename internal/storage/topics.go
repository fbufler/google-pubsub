package storage

import (
	"github.com/fbufler/google-pubsub/internal/domain"
)

func (s *Store) CreateTopic(t *domain.Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[t.Name]; ok {
		return domain.ErrAlreadyExists
	}
	s.topics[t.Name] = t
	s.messages[t.Name] = nil
	return nil
}

func (s *Store) GetTopic(name string) (*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return t, nil
}

func (s *Store) UpdateTopic(t *domain.Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[t.Name]; !ok {
		return domain.ErrNotFound
	}
	s.topics[t.Name] = t
	return nil
}

func (s *Store) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[name]; !ok {
		return domain.ErrNotFound
	}
	delete(s.topics, name)
	delete(s.messages, name)
	return nil
}

func (s *Store) ListTopics(project string) ([]*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := "projects/" + project + "/topics/"
	out := make([]*domain.Topic, 0)
	for _, t := range s.topics {
		if len(t.Name) >= len(prefix) && t.Name[:len(prefix)] == prefix {
			out = append(out, t)
		}
	}
	return out, nil
}

func (s *Store) ListTopicSubscriptions(topicName string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.topics[topicName]; !ok {
		return nil, domain.ErrNotFound
	}
	var names []string
	for _, sub := range s.subscriptions {
		if sub.TopicName == topicName {
			names = append(names, sub.Name)
		}
	}
	return names, nil
}
