package types

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalidArg    = errors.New("invalid argument")
	ErrTopicDeleted  = errors.New("topic deleted")
)

type EntityError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *EntityError) Error() string {
	return e.Message
}

func NewEntityError(code string, message string) *EntityError {
	return &EntityError{Code: code, Message: message}
}

type RepositoryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *RepositoryError) Error() string {
	return e.Message
}

func NewRepositoryError(code string, message string) *RepositoryError {
	return &RepositoryError{Code: code, Message: message}
}
