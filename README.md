# DSend

[![CI](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/ci.yml/badge.svg)](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/ci.yml)
[![Release](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/release.yml/badge.svg)](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/release.yml)

DSend is a lightweight **queue-based distributed message broker** written from scratch in **Go**. Inspired by systems like RabbitMQ, it is built to explore the core concepts behind modern message brokers, including concurrent programming, reliable message delivery, persistence, networking, and distributed systems.

---

## Contents

- [Why DSend?](#why-dsend)
- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Install](#install)
- [Build](#build)
- [Running the Broker](#running-the-broker)
- [Named Queues](#named-queues)
- [Exchanges](#exchanges)
- [Publishing Messages](#publishing-messages)
- [Consuming Messages](#consuming-messages)
- [Consumer Backpressure](#consumer-backpressure)
- [Message Expiry](#message-expiry)
- [Broker Metrics](#broker-metrics)
- [Running Tests](#running-tests)
- [Development](#development)
- [Continuous Integration & Releases](#continuous-integration--releases)
- [Current Capabilities](#current-capabilities)
- [Tech Stack](#tech-stack)
- [License](#license)

---

## Why DSend?

Most production message brokers abstract away the complexity of reliable messaging. DSend was built to understand how those systems work internally by implementing the core building blocks from scratch instead of relying on existing libraries or brokers.

The project focuses on correctness, simplicity, and learning while providing a solid foundation for future distributed features.

---

## Features

### Broker

- Multiple named in-memory ring-buffer queues
- Exchanges with routing (direct, fanout, topic)
- Queue-to-exchange bindings and binding-key matching
- Multi-producer / multi-consumer architecture
- Queue-scoped consumers, round-robin delivery, DLQs, retries, and metrics
- Push-based message delivery
- Round-robin consumer scheduling
- Consumer prefetch & backpressure (slow-consumer protection)
- At-least-once delivery semantics
- Message acknowledgements (ACK)
- Automatic message redelivery
- Per-message TTL / message expiry
- Dead Letter Queue (DLQ)
- Graceful shutdown
- Broker metrics

### Persistence

- Write-Ahead Log (WAL)
- Automatic broker recovery after restart, preserving message queue ownership

### Networking

- Custom TCP server
- Persistent producer connections
- Persistent consumer connections
- JSON-based wire protocol

### Client SDK

- Producer API
- Consumer API
- Metrics API

---

## Architecture

<!-- <img width="1200" height="461" alt="Architecture" src="https://github.com/user-attachments/assets/58781269-7a57-4d71-9afd-add79279020e" /> -->
<img width="1217" height="571" alt="image" src="https://github.com/user-attachments/assets/543b4d25-4ef9-4b12-9247-cc8fdf0b93da" />


---

## Project Structure

```text
client/          Public Go SDK
cmd/dsend/       CLI application
internal/
    engine/      Broker registry and per-queue runtime
    inflight/    In-flight message manager
    protocol/    Wire protocol
    queue/       Ring buffer & DLQ
    server/      TCP server
    session/     Consumer sessions
    storage/     Write-Ahead Log
```

---

## Getting Started

### Prerequisites

- Go 1.25 or later
- GNU Make (optional — used by `make` targets below)

Clone the repository:

```bash
git clone https://github.com/Ali-Hasan-Khan/dsend.git
cd dsend
```

---

## Install

Install the `dsend` CLI binary directly:

```bash
go install github.com/Ali-Hasan-Khan/dsend/cmd/dsend@latest
```

Add the Go SDK as a dependency to your project:

```bash
go get github.com/Ali-Hasan-Khan/dsend
```

Then import it:

```go
import "github.com/Ali-Hasan-Khan/dsend/client"
```

---

## Build

Build the `dsend` binary into `bin/` using the Makefile:

```bash
make build
```

The output is `bin/dsend` on Linux/macOS and `bin/dsend.exe` on Windows. The
binary embeds the current git tag, commit, and build time:

```bash
./bin/dsend version
```

Example output:

```text
dsend v0.2.0-3-gc5245d8
commit: c5245d8
built: 2026-08-05T06:15:37Z
go: go1.26.4
```

To build a single binary without Make, run:

### Linux / macOS

```bash
go build -o dsend ./cmd/dsend
```

### Windows (PowerShell)

```powershell
go build -o dsend.exe .\cmd\dsend
```

---

## Running the Broker

Build and start the broker in one step:

```bash
make run
```

Or start an already-built binary:

### Linux / macOS

```bash
./dsend server
```

### Windows

```powershell
.\dsend.exe server
```

The broker listens on `127.0.0.1:8080` and persists to `./data/wal.log` by default.
Override with flags: `./dsend server --addr 127.0.0.1:8081 --wal ./data/other.log`.

---

## Named Queues

DSend supports multiple isolated named queues. Each queue has its own capacity,
consumers, delivery scheduling, in-flight messages, dead-letter queue, and
metrics. Only the `default` queue exists at startup — create named queues before
publishing to them. Messages are published to an exchange and routed to bound
queues by routing key (see [Exchanges](#exchanges) below).

## Creating Queues

```bash
dsend queue create orders
```

Bind a queue to an exchange with a binding key so it can receive messages:

```bash
dsend queue bind default orders orders
```

List and delete queues:

```bash
dsend queue list
dsend queue delete orders
```

## Exchanges

An exchange routes a published message to every bound queue whose binding key
matches the publish's routing key. Three exchange types are supported:

| Type     | Routing rule                                                            |
| -------- | ----------------------------------------------------------------------- |
| `direct` | Routing key must exactly match the binding key.                         |
| `fanout` | Every bound queue receives every message; the routing key is ignored.   |
| `topic`  | Binding keys support `*` (one segment) and `#` (zero or more segments). |

The `default` exchange (type `direct`) is created at startup and pre-bound to
the `default` queue under the binding key `default`. It cannot be created again
or deleted.

```bash
dsend exchange create events topic
dsend exchange list
dsend exchange delete events
```

## Publishing Messages

A publish targets an exchange and carries a routing key (via `--routing-key`, default `default`):

### Linux / macOS

```bash
./dsend publish --exchange default --routing-key orders "Hello, DSend!"
```

### Windows

```powershell
.\dsend.exe publish --exchange default --routing-key orders "Hello, DSend!"
```

Publishing a routing key that matches no binding returns `no route found`.

---

## Consuming Messages

### Linux / macOS

```bash
./dsend subscribe --queue orders
```

### Windows

```powershell
.\dsend.exe subscribe --queue orders
```

Messages are automatically acknowledged after successful processing. Omitting
`--queue` subscribes to the compatibility `default` queue (no queue creation
required). To reach the default queue without creating anything, publish without any arguments:

```text
dsend publish "Hello, DSend!"
dsend subscribe
```

---

## Consumer Backpressure

The broker caps how many unacknowledged deliveries each consumer session may have
outstanding — the **prefetch count** (default 10). A consumer at its limit is
skipped by the scheduler until it acknowledges a delivery, so a slow consumer
can't be flooded with more work than it can handle, and a fast consumer isn't
starved behind a slow one. Messages bound for a saturated consumer stay queued
instead of being pushed.

The prefetch count is a broker-level setting in `engine.DefaultConfig()`
(`ConsumerPrefetch`). Lower it to limit a consumer's outstanding work; raise it
to let faster consumers buffer more messages.

---

## Message Expiry

Messages can be given a **time-to-live (TTL)** at publish time. A message whose
TTL has elapsed is considered expired and is dead-lettered before it can be
delivered — it is moved to the queue's DLQ and reflected in `DlqCount`.

Publish a message that expires after 10 seconds:

```bash
./dsend publish --exchange default --routing-key orders --ttl 10s "Hello, DSend!"
```

`--ttl` accepts Go duration strings (`10s`, `5m`, `1h30m`). All flags
(`--exchange`, `--routing-key`, `--ttl`, `--addr`) must be given before the
`<payload>` argument — Go `flag` parsing stops at the first positional.
Messages published without `--ttl` never expire.

Expiry is anchored to the **broker's clock**: when a message is accepted, the
broker computes `ExpiryAt = now + ttl`. Expiry is enforced at delivery time, so
an expired message is never handed to a consumer, and a dedicated expiry worker
sweeps every queue on a timer (`ExpiryInterval`, default 1s), clearing expired
messages even when no consumer is connected or the broker is otherwise idle.

The Go SDK exposes the same capability — pass a `time.Duration` as the final
argument to `Producer.Publish` (or `0` for no expiry):

```go
err := p.Publish(ctx, "default", "orders", "order-1042 created", 10*time.Second)
```

---

## Broker Metrics

### Linux / macOS

```bash
./dsend metrics
```

### Windows

```powershell
.\dsend.exe metrics
```

Example output:

```text
ProducedCount: 10
QueueDepth: 0
InflightCount: 0
DlqCount: 0
ConsumerSessionCount: 1
AckedCount: 10
RedeliveredCount: 0
```

---

## Running Tests

Run all tests:

```bash
make test
```

Run the race detector:

```bash
make test-race
```

Generate a coverage report (`coverage.html`):

```bash
make coverage
```

The equivalent plain Go commands are `go test ./...` and `go test -race ./...`.

---

## Development

The Makefile wraps the common development loop. Run `make` (or `make help`) to
list all targets:

```text
$ make
Usage:
  make <target>

Targets:
   Development
    dev                 Run the full local development loop.
    all                 Run quality checks, tests, and the build.
   Quality
    check-quality       Run all code quality checks.
    lint                Run the linter (requires golangci-lint).
    vet                 Run go vet.
    fmt                 Fail if any Go source file is not formatted.
    fmt-fix             Rewrite Go source files with gofmt.
    tidy                Tidy go.mod and go.sum.
   Test
    test                Run all tests.
    test-race           Run all tests with the race detector.
    coverage            Run tests with coverage and emit an HTML report.
   Build
    build               Build the dsend binary into ./bin.
    run                 Build and run the broker server.
    release             Cross-compile release binaries into ./dist.
   Maintenance
    clean               Remove build and test artifacts.
    help                Show this help.
```

Run everything — tidy, format check, vet, tests, and build:

```bash
make dev
```

---

## Continuous Integration & Releases

GitHub Actions is configured in [`.github/workflows`](.github/workflows):

- **CI** — runs on pushes to `main` and pull requests: format check, `go vet`,
  unit tests, race detector, build, and a real broker round-trip
  (create exchange → bind queue → publish → subscribe).
- **Release** — pushing a version tag (`git tag v0.3.0 && git push --tags`)
  cross-compiles binaries for Linux, macOS, and Windows into `dist/`, and
  publishes a GitHub Release with the artifacts.

---

## Current Capabilities

- Multiple named, isolated queues
- Exchanges and routing (direct, fanout, topic)
- Queue-to-exchange bindings
- Reliable message delivery
- ACK-based message processing
- Automatic retry on ACK timeout
- Dead Letter Queue (DLQ)
- Per-message TTL / message expiry
- Round-robin consumer load balancing
- Consumer prefetch & backpressure
- Persistent storage using Write-Ahead Logging
- Automatic recovery after broker restart
- Runtime metrics
- Graceful shutdown
- Concurrent producer and consumer support

<!--
## Roadmap

### v0.3

- Exchanges
- Direct exchange
- Fanout exchange
- Topic exchange
- Routing keys

### Future

- Consumer groups
- Broker clustering
- Replication
- Leader election
- Snapshotting
- Persistent indexes
- Authentication & Authorization
- TLS support
- Prometheus metrics
- Web dashboard
-->

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go |
| Networking | TCP |
| Serialization | JSON |
| Persistence | Write-Ahead Log (WAL) |
| Concurrency | Goroutines, Channels, Mutexes |
| Architecture | Queue-based Message Broker |

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
