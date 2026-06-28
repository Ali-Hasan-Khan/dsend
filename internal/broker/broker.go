package broker

import (
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

type Broker interface {
	Publish(message model.Message) error
	Consume() (Delivery, bool)
	Ack(token string) error

	StartRedeliveryWorker()
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
