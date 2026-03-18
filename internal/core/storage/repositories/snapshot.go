package repositories

import (
	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/storage/mappers"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type SnapshotRepository struct {
	state *memory.State
}

func NewSnapshotRepository(state *memory.State) *SnapshotRepository {
	return &SnapshotRepository{state: state}
}

func (s *SnapshotRepository) CreateSnapshot(snapshot *entities.Snapshot) error {
	model, err := mappers.SnapshotEntityToModel(snapshot)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map snapshot", err)
	}
	if _, ok := s.state.Snapshots[snapshot.Name()]; ok {
		return types.NewPersistenceError(types.PersistenceAlreadyExists, "snapshot already exists")
	}
	s.state.Snapshots[snapshot.Name()] = model
	return nil
}

func (s *SnapshotRepository) GetSnapshot(name types.FQDN) (*entities.Snapshot, error) {
	snapshot, ok := s.state.Snapshots[name]
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "snapshot not found")
	}
	entity, err := mappers.SnapshotModelToEntity(snapshot)
	if err != nil {
		return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map snapshot", err)
	}
	return entity, nil
}

func (s *SnapshotRepository) UpdateSnapshot(snapshot *entities.Snapshot) error {
	model, err := mappers.SnapshotEntityToModel(snapshot)
	if err != nil {
		return types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map snapshot", err)
	}
	if _, ok := s.state.Snapshots[snapshot.Name()]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "snapshot not found")
	}
	s.state.Snapshots[snapshot.Name()] = model
	return nil
}

func (s *SnapshotRepository) DeleteSnapshot(name types.FQDN) error {
	if _, ok := s.state.Snapshots[name]; !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "snapshot not found")
	}
	delete(s.state.Snapshots, name)
	return nil
}

func (s *SnapshotRepository) ListSnapshots(project string) ([]*entities.Snapshot, error) {
	prefix := types.FQDN("projects/" + project + "/snapshots/")
	if !prefix.IsValid() {
		return nil, types.NewPersistenceError(types.PersistencePreconditionFailed, "invalid project name")
	}

	var out []*models.Snapshot
	for _, snap := range s.state.Snapshots {
		if len(snap.Name) >= len(prefix) && snap.Name[:len(prefix)] == prefix {
			out = append(out, snap)
		}
	}

	result := make([]*entities.Snapshot, len(out))
	for i, snap := range out {
		var err error
		result[i], err = mappers.SnapshotModelToEntity(snap)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map snapshot", err)
		}
	}
	return result, nil
}

func (s *SnapshotRepository) ListSnapshotsByTopic(topicName types.FQDN) ([]*entities.Snapshot, error) {
	var out []*models.Snapshot
	for _, snap := range s.state.Snapshots {
		if snap.TopicName == topicName {
			out = append(out, snap)
		}
	}

	result := make([]*entities.Snapshot, len(out))
	for i, snap := range out {
		var err error
		result[i], err = mappers.SnapshotModelToEntity(snap)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map snapshot", err)
		}
	}
	return result, nil
}
