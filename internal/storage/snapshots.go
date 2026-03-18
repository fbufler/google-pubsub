package storage

import (
	"github.com/fbufler/google-pubsub/internal/domain"
)

func (s *Store) CreateSnapshot(snap *domain.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.snapshots[snap.Name]; ok {
		return domain.ErrAlreadyExists
	}
	s.snapshots[snap.Name] = snap
	return nil
}

func (s *Store) GetSnapshot(name string) (*domain.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.snapshots[name]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return snap, nil
}

func (s *Store) UpdateSnapshot(snap *domain.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.snapshots[snap.Name]; !ok {
		return domain.ErrNotFound
	}
	s.snapshots[snap.Name] = snap
	return nil
}

func (s *Store) DeleteSnapshot(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.snapshots[name]; !ok {
		return domain.ErrNotFound
	}
	delete(s.snapshots, name)
	return nil
}

func (s *Store) ListSnapshots(project string) ([]*domain.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := "projects/" + project + "/snapshots/"
	out := make([]*domain.Snapshot, 0)
	for _, snap := range s.snapshots {
		if len(snap.Name) >= len(prefix) && snap.Name[:len(prefix)] == prefix {
			out = append(out, snap)
		}
	}
	return out, nil
}

func (s *Store) ListSnapshotsByTopic(topicName string) ([]*domain.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*domain.Snapshot
	for _, snap := range s.snapshots {
		if snap.TopicName == topicName {
			out = append(out, snap)
		}
	}
	return out, nil
}
