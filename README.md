# DSend

[![CI](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/ci.yml/badge.svg)](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/ci.yml)
[![Release](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/release.yml/badge.svg)](https://github.com/Ali-Hasan-Khan/dsend/actions/workflows/release.yml)

DSend is a lightweight **queue-based distributed message broker** written from scratch in **Go**. Inspired by systems like RabbitMQ, it is built to explore the core concepts behind modern message brokers, including concurrent programming, reliable message delivery, persistence, networking, and distributed systems.

---

## Why DSend?

Most production message brokers abstract away the complexity of reliable messaging. DSend was built to understand how those systems work internally by implementing the core building blocks from scratch instead of relying on existing libraries or brokers.

The project focuses on correctness, simplicity, and learning while providing a solid foundation for future distributed features.

---

## Features

### Broker

- Multiple named in-memory ring-buffer queues
- Multi-producer / multi-consumer architecture
- Queue-scoped consumers, round-robin delivery, DLQs, retries, and metrics
- Push-based message delivery
- Round-robin consumer scheduling
- At-least-once delivery semantics
- Message acknowledgements (ACK)
- Automatic message redelivery
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

The broker listens on `127.0.0.1:8080` and persists to `./data/wal.log`.

---

## Named Queues

DSend supports multiple isolated named queues. Each queue has its own capacity,
consumers, delivery scheduling, in-flight messages, dead-letter queue, and
metrics. Only the `default` queue exists at startup — create named queues before
publishing to them. Publishing or consuming without a queue name uses the
`default` queue.

## Creating Queues

```bash
dsend queue create orders
```

List and delete queues:

```bash
dsend queue list
dsend queue delete orders
```

## Publishing Messages

### Linux / macOS

```bash
./dsend publish --queue orders "Hello, DSend!"
```

### Windows

```powershell
.\dsend.exe publish --queue orders "Hello, DSend!"
```

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

Messages are automatically acknowledged after successful processing. The
following commands target the compatibility `default` queue when `--queue` is
omitted (no queue creation required):

```text
dsend publish "Hello, DSend!"
dsend subscribe
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
  (create queue → publish → subscribe).
- **Release** — pushing a version tag (`git tag v0.3.0 && git push --tags`)
  cross-compiles binaries for Linux, macOS, and Windows into `dist/`, and
  publishes a GitHub Release with the artifacts.

---

## Current Capabilities

- Multiple named, isolated queues
- Reliable message delivery
- ACK-based message processing
- Automatic retry on ACK timeout
- Dead Letter Queue (DLQ)
- Round-robin consumer load balancing
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
