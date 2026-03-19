package entities

import "github.com/fbufler/google-pubsub/internal/core/types"

var (
	ErrMessageDataTooLarge                 = types.NewEntityError("INVALID_MESSAGE_DATA", "message data too large")
	ErrMessageAttributesTooMany            = types.NewEntityError("INVALID_MESSAGE_ATTRIBUTES", "too many attributes")
	ErrMessageAttributeKeyInvalid          = types.NewEntityError("INVALID_MESSAGE_ATTRIBUTE_KEY", "attribute key invalid")
	ErrMessageAttributeValueInvalid        = types.NewEntityError("INVALID_MESSAGE_ATTRIBUTE_VALUE", "attribute value invalid")
	ErrMessageOrderingKeyInvalid           = types.NewEntityError("INVALID_MESSAGE_ORDERING_KEY", "ordering key invalid")
	ErrInvalidSubscriptionName             = types.NewEntityError("INVALID_SUBSCRIPTION_NAME", "subscription name invalid")
	ErrInvalidSnapshotName                 = types.NewEntityError("INVALID_SNAPSHOT_NAME", "snapshot name invalid")
	ErrInvalidTopicName                    = types.NewEntityError("INVALID_TOPIC_NAME", "topic name invalid")
	ErrInvalidLabels                       = types.NewEntityError("INVALID_LABELS", "labels invalid")
	ErrSnapshotExpireTimeInPast            = types.NewEntityError("INVALID_EXPIRE_TIME", "expire time in past")
	ErrSnapshotExpireTimeTooFar            = types.NewEntityError("INVALID_EXPIRE_TIME", "expire time too far in future")
	ErrInvalidSubscriptionAckDeadline      = types.NewEntityError("INVALID_ACK_DEADLINE", "ack deadline invalid")
	ErrInvalidSubscriptionMessageRetention = types.NewEntityError("INVALID_MESSAGE_RETENTION", "message retention invalid")
	ErrInvalidSubscriptionFilter           = types.NewEntityError("INVALID_FILTER", "filter invalid")
	ErrInvalidSubscriptionDeadLetterPolicy = types.NewEntityError("INVALID_DEAD_LETTER_POLICY", "dead letter policy invalid")
	ErrInvalidSubscriptionRetryPolicy      = types.NewEntityError("INVALID_RETRY_POLICY", "retry policy invalid")
	ErrInvalidTopicMessageRetention        = types.NewEntityError("INVALID_MESSAGE_RETENTION", "message retention invalid")
	ErrInvalidTopicKmsKeyName              = types.NewEntityError("INVALID_KMS_KEY_NAME", "KMS key name invalid")
)
