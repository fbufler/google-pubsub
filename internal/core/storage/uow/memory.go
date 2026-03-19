package uow

import (
	"sync"

	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
)

// MemoryStore holds the shared mutable state and its mutex.
// All Memory[P] instances created from the same store share one consistent view.
type MemoryStore struct {
	mu          sync.RWMutex
	globalState *memory.State
}

func NewMemoryStore(initialState *memory.State) *MemoryStore {
	return &MemoryStore{globalState: initialState}
}

// Memory[P] is a generic UnitOfWork backed by a shared MemoryStore.
// P is the provider interface the usecase depends on.
type Memory[P any] struct {
	store           *MemoryStore
	providerFactory func(*memory.State) P
}

func NewMemoryUoW[P any](store *MemoryStore, factory func(*memory.State) P) *Memory[P] {
	return &Memory[P]{store: store, providerFactory: factory}
}

func (uow *Memory[P]) Do(fn func(P) error) error {
	uow.store.mu.Lock()
	defer uow.store.mu.Unlock()

	txState := uow.store.globalState.Clone()
	provider := uow.providerFactory(txState)

	if err := fn(provider); err != nil {
		return err // rollback: discard txState
	}

	uow.store.globalState = txState // commit
	return nil
}
