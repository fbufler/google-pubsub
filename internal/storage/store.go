package storage

import (
	"sync"

	"github.com/fbufler/google-pubsub/internal/domain"
)

// Store is a thread-safe in-memory store for all PubSub resources.
type Store struct {
	mu            sync.RWMutex
	topics        map[string]*domain.Topic
	subscriptions map[string]*domain.Subscription
	snapshots     map[string]*domain.Snapshot
	// labels keyed by resource name (snapshot, topic, subscription)
	labels map[string]map[string]string
	// messages keyed by topic name, ordered by publish time
	messages map[string][]*domain.Message
	// pending messages keyed by subscription name
	pending map[string][]*domain.PendingMessage
}

func New() *Store {
	return &Store{
		topics:        make(map[string]*domain.Topic),
		subscriptions: make(map[string]*domain.Subscription),
		snapshots:     make(map[string]*domain.Snapshot),
		labels:        make(map[string]map[string]string),
		messages:      make(map[string][]*domain.Message),
		pending:       make(map[string][]*domain.PendingMessage),
	}
}
