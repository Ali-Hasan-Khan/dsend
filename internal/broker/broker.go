package broker

import (
	"context"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

type Broker interface {
	Publish(message model.Message) error
	Ack(token string) error
	Subscribe(session *session.ConsumerSession)
	Unsubscribe(id string)

	StartRedeliveryWorker(ctx context.Context)
	RunDistributor(ctx context.Context)
	Shutdown()

	Metrics() model.Metric
	IsClosed() bool
}

func NewBroker(cfg Config, wal storage.WAL) (Broker, error) {
	msgs, err := wal.Load()
	if err != nil {
		return nil, err
	}

	return NewInMemoryBroker(cfg, msgs, wal), nil
}
