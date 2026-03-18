package entities

import (
	"strings"
	"time"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// Message represents a PubSub message stored in a topic.
type Message struct {
	id          string
	data        []byte
	attributes  map[string]string
	orderingKey string
	publishTime time.Time
}

func (m *Message) ID() string {
	return m.id
}

func (m *Message) SetID(id string) error {
	m.id = id
	return nil
}

func (m *Message) Data() []byte {
	return m.data
}

// SetData sets the data of the message.
// Constraints:
// - Must not exceed 10 MB in size.
func (m *Message) SetData(data []byte) error {
	if len(data) > 10*1024*1024 {
		return ErrMessageDataTooLarge
	}
	m.data = data
	return nil
}

func (m *Message) Attributes() map[string]string {
	return m.attributes
}

// SetAttributes sets the attributes of the message.
// Constraints:
// - Count: Maximum of 100 attributes per message.
// - Keys: Maximum length is 256 bytes. Keys cannot start with the prefix goog.
// - Values: Maximum length is 1024 bytes.
func (m *Message) SetAttributes(attributes map[string]string) error {
	if len(attributes) > 100 {
		return ErrMessageAttributesTooMany
	}
	for k, v := range attributes {
		if len(k) > 256 || strings.HasPrefix(k, "goog.") {
			return ErrMessageAttributeKeyInvalid
		}
		if len(v) > 1024 {
			return ErrMessageAttributeValueInvalid
		}
	}
	m.attributes = attributes
	return nil
}

func (m *Message) OrderingKey() string {
	return m.orderingKey
}

// SetOrderingKey sets the ordering key of the message.
// Constraints:
// - Maximum length is 1024 bytes.
func (m *Message) SetOrderingKey(orderingKey string) error {
	if len(orderingKey) > 1024 {
		return ErrMessageOrderingKeyInvalid
	}
	m.orderingKey = orderingKey
	return nil
}

func (m *Message) PublishTime() time.Time {
	return m.publishTime
}

func (m *Message) SetPublishTime(publishTime time.Time) error {
	m.publishTime = publishTime
	return nil
}

// PendingMessage is a message awaiting acknowledgement on a subscription.
type PendingMessage struct {
	message         *Message
	ackID           string
	deliveryAttempt int32
	ackDeadline     time.Time
	subscription    types.FQDN
}

func (pm *PendingMessage) Message() *Message {
	return pm.message
}

func (pm *PendingMessage) SetMessage(message *Message) error {
	pm.message = message
	return nil
}

func (pm *PendingMessage) AckID() string {
	return pm.ackID
}

func (pm *PendingMessage) SetAckID(ackID string) error {
	pm.ackID = ackID
	return nil
}

func (pm *PendingMessage) DeliveryAttempt() int32 {
	return pm.deliveryAttempt
}

func (pm *PendingMessage) SetDeliveryAttempt(deliveryAttempt int32) error {
	pm.deliveryAttempt = deliveryAttempt
	return nil
}

func (pm *PendingMessage) AckDeadline() time.Time {
	return pm.ackDeadline
}

func (pm *PendingMessage) SetAckDeadline(ackDeadline time.Time) error {
	pm.ackDeadline = ackDeadline
	return nil
}

func (pm *PendingMessage) Subscription() types.FQDN {
	return pm.subscription
}

func (pm *PendingMessage) SetSubscription(subscription types.FQDN) error {
	if !subscription.IsValid() {
		return ErrInvalidSubscriptionName
	}
	pm.subscription = subscription
	return nil
}
