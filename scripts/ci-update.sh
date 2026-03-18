#!/usr/bin/env bash
# ci:update — fetch the latest pubsub proto spec, regenerate, test against both
# emulators, and commit if anything changed and tests pass.
set -euo pipefail

GOOGLEAPIS_BASE="https://raw.githubusercontent.com/googleapis/googleapis/master"
PROTO_DIR="proto/google/pubsub/v1"

echo "==> Fetching latest pubsub proto spec..."
curl -fsSL "${GOOGLEAPIS_BASE}/google/pubsub/v1/pubsub.proto" -o "${PROTO_DIR}/pubsub.proto"
curl -fsSL "${GOOGLEAPIS_BASE}/google/pubsub/v1/schema.proto" -o "${PROTO_DIR}/schema.proto"

echo "==> Updating buf dependencies..."
buf dep update

echo "==> Regenerating Go code..."
buf generate

echo "==> Running go mod tidy..."
go mod tidy

echo "==> Running tests against the real Google emulator..."
task test:integration:official

echo "==> Building our Docker image..."
task docker:build

echo "==> Running tests against our emulator..."
task test:integration:ours

echo "==> Checking for changes to commit..."
if git diff --quiet && git diff --staged --quiet; then
    echo "No changes to commit."
    exit 0
fi

echo "==> Committing updates..."
git add \
    proto/google/pubsub/v1/pubsub.proto \
    proto/google/pubsub/v1/schema.proto \
    gen/ \
    buf.lock \
    go.mod \
    go.sum

git commit -m "chore: update pubsub proto spec and regenerate

Fetched latest google/pubsub/v1 proto from googleapis/googleapis.
All integration tests pass against both the real emulator and ours."

echo "==> Done."
