package models

import (
	"time"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

type Topic struct {
	Name             types.FQDN
	Labels           types.Labels
	MessageRetention time.Duration
	KmsKeyName       types.CMEK
	CreatedAt        time.Time
}
