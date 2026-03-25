package memory

import "sync"

// State holds all emulator state as concurrent-safe maps.
// Snapshots, Topics, Subscriptions, and Messages use sync.Map (FQDN → model pointer).
// Queues maps each subscription FQDN to its *SubscriptionQueue (channel + in-flight).
type State struct {
	Snapshots     sync.Map // types.FQDN → *models.Snapshot
	Topics        sync.Map // types.FQDN → *models.Topic
	Subscriptions sync.Map // types.FQDN → *models.Subscription
	Messages      sync.Map // types.FQDN → *models.Message
	Queues        sync.Map // types.FQDN → *SubscriptionQueue
}
