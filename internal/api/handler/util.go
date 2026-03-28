package handler

import (
	"errors"
	"log/slog"
	"strings"

	"connectrpc.com/connect"

	"github.com/fbufler/google-pubsub/internal/core/types"
)

// projectID extracts the project ID from a resource string like "projects/my-project".
func projectID(s string) string {
	const prefix = "projects/"
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):]
	}
	return s
}

// toConnectError maps a usecase or entity error to the appropriate Connect error code.
func toConnectError(err error) error {
	var ue *types.UsecaseError
	if errors.As(err, &ue) {
		switch ue.Kind {
		case types.UsecaseNotFound:
			return connect.NewError(connect.CodeNotFound, err)
		case types.UsecaseAlreadyExists:
			return connect.NewError(connect.CodeAlreadyExists, err)
		case types.UsecaseInvalidArgument:
			return connect.NewError(connect.CodeInvalidArgument, err)
		case types.UsecasePreconditionFailed:
			return connect.NewError(connect.CodeFailedPrecondition, err)
		default:
			return connect.NewError(connect.CodeInternal, err)
		}
	}
	// Entity validation errors from handler-level setters.
	var ee *types.EntityError
	if errors.As(err, &ee) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

// logErr logs at Error for internal failures, Debug for everything else (4xx).
func logErr(err error, msg string, attrs ...any) {
	var ue *types.UsecaseError
	if errors.As(err, &ue) && ue.Kind == types.UsecaseInternal {
		slog.Error(msg, append([]any{"err", err}, attrs...)...)
		return
	}
	slog.Debug(msg, append([]any{"err", err}, attrs...)...)
}
