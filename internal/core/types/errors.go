package types

import (
	"errors"
	"fmt"
)

// EntityError is returned by entity setters when validation fails.
// It always maps to an invalid-argument condition.
type EntityError struct {
	Code    string
	Message string
}

func (e *EntityError) Error() string { return e.Message }

func NewEntityError(code, message string) *EntityError {
	return &EntityError{Code: code, Message: message}
}

// ---------------------------------------------------------------------------
// Persistence layer
// ---------------------------------------------------------------------------

// PersistenceKind categorises errors returned by repository methods.
type PersistenceKind int

const (
	PersistenceNotFound          PersistenceKind = iota // resource does not exist
	PersistenceAlreadyExists                            // resource already exists
	PersistencePreconditionFailed                       // invalid input (e.g. bad project name)
	PersistenceMappingFailed                            // model ↔ entity conversion failed
	PersistenceInternal                                 // unexpected storage error
)

// PersistenceError is returned exclusively by repository methods.
type PersistenceError struct {
	Kind    PersistenceKind
	Message string
	Cause   error
}

func (e *PersistenceError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *PersistenceError) Unwrap() error { return e.Cause }

func NewPersistenceError(kind PersistenceKind, message string) *PersistenceError {
	return &PersistenceError{Kind: kind, Message: message}
}

func WrapPersistenceError(kind PersistenceKind, message string, cause error) *PersistenceError {
	return &PersistenceError{Kind: kind, Message: message, Cause: cause}
}

// ---------------------------------------------------------------------------
// Usecase layer
// ---------------------------------------------------------------------------

// UsecaseKind categorises errors returned by usecase methods.
type UsecaseKind int

const (
	UsecaseNotFound          UsecaseKind = iota // resource does not exist
	UsecaseAlreadyExists                        // resource already exists
	UsecaseInvalidArgument                      // invalid input
	UsecasePreconditionFailed                   // operation not allowed in current state
	UsecaseInternal                             // unexpected error
)

// UsecaseError is returned exclusively by usecase methods.
type UsecaseError struct {
	Kind    UsecaseKind
	Message string
	Cause   error
}

func (e *UsecaseError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *UsecaseError) Unwrap() error { return e.Cause }

func NewUsecaseError(kind UsecaseKind, message string) *UsecaseError {
	return &UsecaseError{Kind: kind, Message: message}
}

func WrapUsecaseError(kind UsecaseKind, message string, cause error) *UsecaseError {
	return &UsecaseError{Kind: kind, Message: message, Cause: cause}
}

// IsUsecaseKind reports whether err contains a *UsecaseError with the given kind.
func IsUsecaseKind(err error, kind UsecaseKind) bool {
	var ue *UsecaseError
	return errors.As(err, &ue) && ue.Kind == kind
}
