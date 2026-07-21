package engine

import (
	"context"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
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
}

func NewBroker(cfg Config, wal storage.WAL) (Broker, error) {
	msgs, err := wal.Load()
	if err != nil {
		return nil, err
	}

	cap := max(cfg.QueueSize, len(msgs))
	ringQ := queue.NewRingBufferQueue(cap)
	deadQ := queue.NewDLQ()
	inflightMgr := inflight.NewManager()

	return NewInMemoryBroker(cfg, msgs, wal, ringQ, deadQ, inflightMgr), nil
}
