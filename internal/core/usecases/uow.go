package usecases

// UnitOfWork runs a function transactionally against a repository provider P.
// On success the state change is committed; on error it is rolled back.
type UnitOfWork[P any] interface {
	Do(fn func(P) error) error
}
