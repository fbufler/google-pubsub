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
	if _, loaded := s.state.Snapshots.LoadOrStore(snapshot.Name(), model); loaded {
		return types.NewPersistenceError(types.PersistenceAlreadyExists, "snapshot already exists")
	}
	return nil
}

func (s *SnapshotRepository) GetSnapshot(name types.FQDN) (*entities.Snapshot, error) {
	v, ok := s.state.Snapshots.Load(name)
	if !ok {
		return nil, types.NewPersistenceError(types.PersistenceNotFound, "snapshot not found")
	}
	entity, err := mappers.SnapshotModelToEntity(v.(*models.Snapshot))
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
	if _, ok := s.state.Snapshots.Load(snapshot.Name()); !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "snapshot not found")
	}
	s.state.Snapshots.Store(snapshot.Name(), model)
	return nil
}

func (s *SnapshotRepository) DeleteSnapshot(name types.FQDN) error {
	if _, ok := s.state.Snapshots.Load(name); !ok {
		return types.NewPersistenceError(types.PersistenceNotFound, "snapshot not found")
	}
	s.state.Snapshots.Delete(name)
	return nil
}

func (s *SnapshotRepository) ListSnapshots(project string) ([]*entities.Snapshot, error) {
	prefix := types.FQDN("projects/" + project + "/snapshots/")
	if !prefix.IsValid() {
		return nil, types.NewPersistenceError(types.PersistencePreconditionFailed, "invalid project name")
	}

	var raw []*models.Snapshot
	s.state.Snapshots.Range(func(k, v any) bool {
		fqdn := k.(types.FQDN)
		if len(fqdn) >= len(prefix) && fqdn[:len(prefix)] == prefix {
			raw = append(raw, v.(*models.Snapshot))
		}
		return true
	})

	out := make([]*entities.Snapshot, len(raw))
	for i, snap := range raw {
		var err error
		out[i], err = mappers.SnapshotModelToEntity(snap)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map snapshot", err)
		}
	}
	return out, nil
}

func (s *SnapshotRepository) ListSnapshotsByTopic(topicName types.FQDN) ([]*entities.Snapshot, error) {
	var raw []*models.Snapshot
	s.state.Snapshots.Range(func(_, v any) bool {
		snap := v.(*models.Snapshot)
		if snap.TopicName == topicName {
			raw = append(raw, snap)
		}
		return true
	})

	out := make([]*entities.Snapshot, len(raw))
	for i, snap := range raw {
		var err error
		out[i], err = mappers.SnapshotModelToEntity(snap)
		if err != nil {
			return nil, types.WrapPersistenceError(types.PersistenceMappingFailed, "failed to map snapshot", err)
		}
	}
	return out, nil
}
