## Dsend

Dsend is a queue-based Distributed Message Queue system(similar to RabbitMQ) built using Golang from scratch.

### Current Architecture
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

### Target Architecture (Completed Broker)

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
