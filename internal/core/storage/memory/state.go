package memory

import (
	"github.com/fbufler/google-pubsub/internal/core/storage/models"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

type State struct {
	Snapshots       map[types.FQDN]*models.Snapshot
	Topics          map[types.FQDN]*models.Topic
	Subscriptions   map[types.FQDN]*models.Subscription
	Messages        map[types.FQDN]*models.Message
	PendingMessages map[types.FQDN][]*models.PendingMessage
}

// Clone creates a deep copy for transaction isolation
func (s *State) Clone() *State {
	newSnapshots := make(map[types.FQDN]*models.Snapshot, len(s.Snapshots))
	for k, v := range s.Snapshots {
		newSnapshots[k] = v
	}

	newTopics := make(map[types.FQDN]*models.Topic, len(s.Topics))
	for k, v := range s.Topics {
		newTopics[k] = v
	}

	newSubscriptions := make(map[types.FQDN]*models.Subscription, len(s.Subscriptions))
	for k, v := range s.Subscriptions {
		newSubscriptions[k] = v
	}

	newMessages := make(map[types.FQDN]*models.Message, len(s.Messages))
	for k, v := range s.Messages {
		newMessages[k] = v
	}

	newPendingMessages := make(map[types.FQDN][]*models.PendingMessage, len(s.PendingMessages))
	for k, v := range s.PendingMessages {
		cp := make([]*models.PendingMessage, len(v))
		copy(cp, v)
		newPendingMessages[k] = cp
	}

	return &State{
		Snapshots:       newSnapshots,
		Topics:          newTopics,
		Subscriptions:   newSubscriptions,
		Messages:        newMessages,
		PendingMessages: newPendingMessages,
	}
}
