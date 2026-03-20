package telemetry

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// SetupLogging configures the default slog logger.
// level: debug|info|warn|error (case-insensitive)
// format: text|json
func SetupLogging(level, format string) error {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return fmt.Errorf("unknown log level %q: use debug|info|warn|error", level)
	}

	opts := &slog.HandlerOptions{Level: l}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	case "text", "":
		handler = slog.NewTextHandler(os.Stderr, opts)
	default:
		return fmt.Errorf("unknown log format %q: use text|json", format)
	}

	slog.SetDefault(slog.New(handler))
	return nil
}
