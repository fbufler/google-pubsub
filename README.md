# google-pubsub-emulator

A lightweight Google Cloud Pub/Sub emulator written in pure Go.

The official Google emulator is a Java application packaged in a ~600 MB Docker image. This project implements the same gRPC API in Go, producing a **really small scratch-based image** that is fully compatible with the official [`cloud.google.com/go/pubsub`](https://pkg.go.dev/cloud.google.com/go/pubsub) client library.

## Features

- Full Publisher API — topics, publish, field-mask updates, retention
- Full Subscriber API — pull, streaming pull, ack/nack, dead-letter, ordering
- Snapshots — create, seek-to-snapshot, seek-to-time
- Init config — pre-create topics and subscriptions on startup
- Single static binary, scratch Docker image
- Validated against the real Google emulator with the same integration test suite

## Quick start

```bash
docker run --rm -p 8085:8085 ghcr.io/fbufler/google-pubsub:latest
```

Point your client at it:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
```

### With an init config

Pre-create topics and subscriptions on startup by mounting a YAML file:

```yaml
# init.yaml
projects:
    - id: my-project
      topics:
          - name: orders
            subscriptions:
                - name: orders-processor
                  ack_deadline_seconds: 30
```

```bash
docker run --rm -p 8085:8085 \
  -e INIT_CONFIG=/etc/pubsub/init.yaml \
  -v $(pwd)/init.yaml:/etc/pubsub/init.yaml:ro \
  ghcr.io/fbufler/google-pubsub:latest
```

### docker-compose

```yaml
services:
    pubsub:
        image: ghcr.io/fbufler/google-pubsub:latest
        ports:
            - "8085:8085"
        environment:
            - INIT_CONFIG=/etc/pubsub/init.yaml
        volumes:
            - ./init.yaml:/etc/pubsub/init.yaml:ro
```

## Configuration

| Environment variable | Default   | Description                     |
| -------------------- | --------- | ------------------------------- |
| `LISTEN_ADDR`        | `:8085`   | Address the server listens on   |
| `INIT_CONFIG`        | _(unset)_ | Path to a YAML init config file |

## Development

### Prerequisites

Install [mise](https://mise.jdx.dev/) then run:

```bash
mise install
```

This installs Go, Task, buf, and golangci-lint at the versions defined in `mise.toml`.

### Proto toolchain

Proto code is generated with [buf](https://buf.build) using the [connect-go](https://connectrpc.com/docs/go/getting-started/) plugin, which produces clean idiomatic Go and is fully gRPC-compatible.

```bash
task proto:update    # pull latest spec from googleapis/googleapis
task proto:generate  # regenerate → gen/
```

### Integration tests

The test suite in `test/integration/` runs against a live emulator. It is skipped by default and enabled with the `integration` build tag.

To validate against the **real** Google emulator:

```bash
task test:integration:official
```

To validate against **our** emulator:

```bash
task docker:build
task test:integration:ours
```

All CI pipelines use [mise](https://mise.jdx.dev/) (`jdx/mise-action`) to install tools.

## License

Apache 2.0 and Beerware — if you use this and we ever meet, buy me a beer. 🍺
