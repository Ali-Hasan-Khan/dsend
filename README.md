## Dsend

Dsend is a **queue-based distributed message broker** (inspired by RabbitMQ) built completely from scratch in **Go**.


---

## Features Implemented

### Core Broker

- In-memory ring-buffer queue
- Multi-producer / multi-consumer support
- Push-based message delivery
- Round-robin consumer scheduling
- At-least-once delivery semantics
- Ack-based message processing
- Automatic message redelivery
- Dead Letter Queue (DLQ)
- Queue depth & broker metrics

### Persistence

- Write Ahead Log (WAL)
- Broker recovery from WAL after restart

### Networking

- Custom TCP server
- Persistent producer connections
- Persistent consumer connections
- JSON-based wire protocol
- Consumer subscribe / unsubscribe
- Graceful shutdown

---

## Current Architecture

<img width="1200" height="461" alt="image" src="https://github.com/user-attachments/assets/58781269-7a57-4d71-9afd-add79279020e" />

<!--
```text
                           ┌─────────────────────┐
                           │      Producers      │
                           └──────────┬──────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────┐
                    │            Broker               │
                    │                                 │
                    │  ┌───────────────────────────┐  │
                    │  │        Ready Queue        │  │
                    │  │   (Circular Buffer)       │  │
                    │  └─────────────┬─────────────┘  │
                    │                │                │
                    │                ▼                │
                    │  ┌───────────────────────────┐  │
                    │  │     Delivery Manager      │  │
                    │  │  Generates Ack Tokens     │  │
                    │  └─────────────┬─────────────┘  │
                    │                │                │
                    │                ▼                │
                    │  ┌───────────────────────────┐  │
                    │  │      InFlight Store       │  │
                    │  │ token -> delivery state   │  │
                    │  └─────────────┬─────────────┘  │
                    │                │                │
                    │      timeout   │   ack(token)   │
                    │                │                │
                    │                ▼                │
                    │  ┌───────────────────────────┐  │
                    │  │    Redelivery Manager     │  │
                    │  └─────────────┬─────────────┘  │
                    │                │                │
                    │     retry      │     maxRetry   │
                    │                ▼                │
                    │  ┌───────────────────────────┐  │
                    │  │    Dead Letter Queue      │  │
                    │  └───────────────────────────┘  │
                    └─────────────────────────────────┘
                                      │
                                      ▼
                           ┌─────────────────────┐
                           │      Consumers      │
                           └─────────────────────┘
```
-->

## Target Architecture (Distributed Broker)

```text
                                    ┌──────────────────┐
                                    │     Producers    │
                                    └────────┬─────────┘
                                             │
                                             ▼
                         ┌────────────────────────────────────┐
                         │         Broker TCP Server          │
                         └────────────────┬───────────────────┘
                                          │
                                          ▼
                 ┌───────────────────────────────────────────────┐
                 │                 Broker Core                   │
                 │                                               │
                 │  ┌────────────────────────────────────────┐   │
                 │  │            Exchange Layer              │   │
                 │  │                                        │   │
                 │  │  Direct Exchange                       │   │
                 │  │  Fanout Exchange                       │   │
                 │  │  Topic Exchange                        │   │
                 │  └───────────────┬────────────────────────┘   │
                 │                  │                            │
                 │                  ▼                            │
                 │  ┌────────────────────────────────────────┐   │
                 │  │          Queue Registry                │   │
                 │  │                                        │   │
                 │  │  Queue A                               │   │
                 │  │  Queue B                               │   │
                 │  │  Queue C                               │   │
                 │  │  ...                                   │   │
                 │  └───────────────┬────────────────────────┘   │
                 │                  │                            │
                 │                  ▼                            │
                 │  ┌────────────────────────────────────────┐   │
                 │  │           Queue Engine                 │   │
                 │  │                                        │   │
                 │  │  Ready Queue                           │   │
                 │  │  InFlight Store                        │   │
                 │  │  Retry Manager                         │   │
                 │  │  Dead Letter Queue                     │   │
                 │  │  Consumer Group Manager                │   │
                 │  └───────────────┬────────────────────────┘   │
                 │                  │                            │
                 │                  ▼                            │
                 │  ┌────────────────────────────────────────┐   │
                 │  │          Persistence Layer             │   │
                 │  │                                        │   │
                 │  │  Write Ahead Log (WAL)                 │   │
                 │  │  Snapshot Storage                      │   │
                 │  │  Recovery Manager                      │   │
                 │  └───────────────┬────────────────────────┘   │
                 └──────────────────┼────────────────────────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      Consumers       │
                         └──────────────────────┘


                          (Future Distributed Mode)

       ┌─────────────┐        Replication       ┌─────────────┐
       │  Broker A   │ ◄──────────────────────► │  Broker B   │
       └──────┬──────┘                          └───────┬─────┘
              │                                         │
              └────────────── Cluster ──────────────────┘
                                │
                                ▼
                     Leader Election / Failover
```

---

## Tech Stack

- **Language:** Go
- **Networking:** TCP
- **Concurrency:** Goroutines, Channels, Mutexes
- **Persistence:** Write Ahead Log (WAL)
- **Serialization:** JSON
- **Architecture:** Queue-based Message Broker
