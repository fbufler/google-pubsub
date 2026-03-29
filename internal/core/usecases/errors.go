package usecases

import (
	"errors"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// fromPersistence translates a repository-layer *PersistenceError into a
// usecase-layer *UsecaseError. Any other error type becomes UsecaseInternal.
func fromPersistence(err error) *types.UsecaseError {
	var pe *types.PersistenceError
	if errors.As(err, &pe) {
		switch pe.Kind {
		case types.PersistenceNotFound:
			return types.WrapUsecaseError(types.UsecaseNotFound, pe.Message, pe)
		case types.PersistenceAlreadyExists:
			return types.WrapUsecaseError(types.UsecaseAlreadyExists, pe.Message, pe)
		case types.PersistencePreconditionFailed:
			return types.WrapUsecaseError(types.UsecaseInvalidArgument, pe.Message, pe)
		case types.PersistenceResourceExhausted:
			return types.WrapUsecaseError(types.UsecaseResourceExhausted, pe.Message, pe)
		case types.PersistenceMappingFailed, types.PersistenceInternal:
			return types.WrapUsecaseError(types.UsecaseInternal, pe.Message, pe)
		}
	}
	return types.WrapUsecaseError(types.UsecaseInternal, "unexpected error", err)
}
