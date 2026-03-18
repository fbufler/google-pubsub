package repositories

import "github.com/fbufler/google-pubsub/internal/core/storage/memory"

// RepositoryProvider exposes all repositories scoped to one unit-of-work transaction.
type RepositoryProvider interface {
	Topics() *TopicRepository
	Subscriptions() *SubscriptionRepository
	Messages() *MessageRepository
	PendingMessages() *PendingMessageRepository
	Snapshots() *SnapshotRepository
}

// Provider is the in-memory implementation of RepositoryProvider.
type Provider struct {
	state *memory.State
}

func NewProvider(state *memory.State) *Provider {
	return &Provider{state: state}
}

func (p *Provider) Topics() *TopicRepository {
	return NewTopicRepository(p.state)
}

func (p *Provider) Subscriptions() *SubscriptionRepository {
	return NewSubscriptionRepository(p.state)
}

func (p *Provider) Messages() *MessageRepository {
	return NewMessageRepository(p.state)
}

func (p *Provider) PendingMessages() *PendingMessageRepository {
	return NewPendingMessageRepository(p.state)
}

func (p *Provider) Snapshots() *SnapshotRepository {
	return NewSnapshotRepository(p.state)
}
