# DSend

DSend is a lightweight **queue-based distributed message broker** written from scratch in **Go**. Inspired by systems like RabbitMQ, it is built to explore the core concepts behind modern message brokers, including concurrent programming, reliable message delivery, persistence, networking, and distributed systems.

---

## Why DSend?

Most production message brokers abstract away the complexity of reliable messaging. DSend was built to understand how those systems work internally by implementing the core building blocks from scratch instead of relying on existing libraries or brokers.

The project focuses on correctness, simplicity, and learning while providing a solid foundation for future distributed features.

---

## Features

### Broker

- In-memory ring buffer queue
- Multi-producer / multi-consumer architecture
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
- Automatic broker recovery after restart

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

<img width="1200" height="461" alt="Architecture" src="https://github.com/user-attachments/assets/58781269-7a57-4d71-9afd-add79279020e" />

---

## Project Structure

```text
client/          Public Go SDK
cmd/dsend/       CLI application
internal/
    engine/      Broker core
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

- Go 1.24 or later

Clone the repository:

```bash
git clone https://github.com/Ali-Hasan-Khan/dsend.git
cd dsend
```

---

## Build

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

### Linux / macOS

```bash
./dsend server
```

### Windows

```powershell
.\dsend.exe server
```

---

## Publishing Messages

### Linux / macOS

```bash
./dsend publish "Hello, DSend!"
```

### Windows

```powershell
.\dsend.exe publish "Hello, DSend!"
```

---

## Consuming Messages

### Linux / macOS

```bash
./dsend subscribe
```

### Windows

```powershell
.\dsend.exe subscribe
```

Messages are automatically acknowledged after successful processing.

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
go test ./...
```

Run the race detector:

```bash
go test -race ./...
```

---

## Current Capabilities

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

### v0.2

- Multiple named queues
- Queue management API
- Configurable broker settings
- Improved CLI

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