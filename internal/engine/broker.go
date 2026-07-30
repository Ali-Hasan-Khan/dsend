package engine

import (
	"context"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

type Broker interface {
	CreateQueue(name string) error
	DeleteQueue(name string) error
	ListQueues() []string

	Publish(queueName string, message model.Message) error
	Ack(token string) error
	Subscribe(queueName string, session *session.ConsumerSession) error
	Unsubscribe(queueName, sessionID string)

	QueueMetrics(queueName string) (model.Metric, error)
	Metrics() model.BrokerMetrics

	Start(ctx context.Context)
	Shutdown()
}

func NewBroker(cfg Config, wal storage.WAL) (Broker, error) {
	state, err := wal.Load()
	if err != nil {
		return nil, err
	}

	broker := NewInMemoryBroker(
		cfg,
		wal,
		make(map[string]*QueueRuntime),
		make(map[string]*QueueRuntime),
	)

	for queueName, messages := range state.PendingMessages {
		broker.queues[queueName] = broker.newQueueRuntime(queueName, messages)
	}

	if _, exists := broker.queues[model.DefaultQueueName]; !exists {
		broker.queues[model.DefaultQueueName] = broker.newQueueRuntime(model.DefaultQueueName, nil)
	}

	return broker, nil
}
