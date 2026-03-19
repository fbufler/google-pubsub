package handler

import "strings"

// projectID extracts the project ID from a resource string like "projects/my-project".
func projectID(s string) string {
	const prefix = "projects/"
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):]
	}
	return s
}
