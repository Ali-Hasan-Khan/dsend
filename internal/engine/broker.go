package engine

import (
	"context"
	"errors"

	"github.com/Ali-Hasan-Khan/dsend/internal/exchange"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

type Broker interface {
	CreateExchange(name string, exchangeType string) error
	DeleteExchange(name string) error
	ListExchanges() []string

	CreateQueue(name string) error
	DeleteQueue(name string) error
	ListQueues() []string
	BindQueue(exchangeName, queueName, bindingKey string) error
	UnbindQueue(exchangeName, queueName, bindingKey string) error

	Publish(exchangeName, routingKey string, payload model.Message) error
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

	for exchangeName, exchangeState := range state.PendingExchanges {
		if err := broker.CreateExchange(exchangeName, exchangeState.ExchangeType); err != nil {
			return broker, err
		}

		for _, binding := range exchangeState.Bindings {
			if err := broker.BindQueue(exchangeName, binding.QueueName, binding.BindingKey); err != nil {
				return broker, err
			}
		}
	}

	if err := broker.BindQueue(model.DefaultExchangeName, model.DefaultQueueName, model.DefaultQueueName); err != nil {
		if !errors.Is(err, exchange.ErrBindingAlreadyExist) {
			return broker, err
		}
	}

	return broker, nil
}
