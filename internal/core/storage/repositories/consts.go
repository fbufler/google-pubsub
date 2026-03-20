package repositories

import "github.com/fbufler/google-pubsub/internal/core/types"

var (
	// ErrInvalidProjectName is returned when a project name fails validation.
	ErrInvalidProjectName = types.NewRepositoryError("PRECONDITION_FAILED", "invalid project name")
	// ErrNotFound aliases the domain sentinel so handler equality checks work.
	ErrNotFound = types.ErrNotFound
)
