package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Ali-Hasan-Khan/dsend/internal/dlq"
	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

var ErrBrokerClosed = errors.New("broker closed")

type Queue interface {
	Push(model.Message)
	Pop() model.Message
	Size() int
	Capacity() int
}

type DeadLetterQueue interface {
	Push(model.Message)
	Size() int
}

type InMemoryBroker struct {
	queue Queue
	mu    sync.Mutex

	closed bool

	condProd *sync.Cond
	condCons *sync.Cond

	inFlightManager *inflight.Manager
	deadLetterQueue DeadLetterQueue

	ackedCount    int
	producedCount int

	wal storage.WAL

	config Config
}

func NewInMemoryBroker(cfg Config, messages []model.Message, wal storage.WAL) *InMemoryBroker {
	cap := max(cfg.QueueSize, len(messages))
	broker := &InMemoryBroker{
		queue:           queue.NewRingBufferQueue(cap),
		inFlightManager: inflight.NewManager(),
		deadLetterQueue: dlq.NewDLQ(),
		wal:             wal,
		config:          cfg,
	}

	broker.condProd = sync.NewCond(&broker.mu)
	broker.condCons = sync.NewCond(&broker.mu)

	for _, msg := range messages {
		broker.queue.Push(msg)
		broker.producedCount++
	}
	return broker
}

func (q *InMemoryBroker) Publish(message model.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.queue.Size() == q.queue.Capacity() {
		q.condProd.Wait()
	}
	if q.closed {
		return ErrBrokerClosed
	}
	if message.ID == "" {
		message.ID = uuid.NewString()
	}
	message.Timestamp = time.Now().UTC()
	if err := q.wal.Append(message); err != nil {
		return err
	}
	q.queue.Push(message)
	q.producedCount++
	q.condCons.Signal()

	return nil
}

func (q *InMemoryBroker) Consume() (Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.queue.Size() == 0 {
		q.condCons.Wait()
	}
	if q.closed && q.queue.Size() == 0 {
		return Delivery{}, false
	}
	token := uuid.NewString()
	message := q.queue.Pop()
	q.inFlightManager.Add(token, message)
	q.condProd.Signal()
	delivery := Delivery{
		Message:  message,
		AckToken: token,
	}
	return delivery, true
}

func (q *InMemoryBroker) Ack(token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if ok := q.inFlightManager.IsPresent(token); !ok {
		fmt.Printf("Token Not found: %s\n", token)
		return fmt.Errorf("Message not present in inflight")
	}
	q.inFlightManager.Remove(token)
	q.ackedCount++
	return nil
}

func (q *InMemoryBroker) StartRedeliveryWorker(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for !q.IsClosed() {
		select {
		case <-ticker.C:
			q.processExpiredMessages()
		case <-ctx.Done():
			return
		}
	}
}

func (q *InMemoryBroker) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.condProd.Broadcast()
	q.condCons.Broadcast()
}

func (q *InMemoryBroker) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *InMemoryBroker) Metrics() model.Metric {
	q.mu.Lock()
	defer q.mu.Unlock()
	metrics := model.Metric{
		AckedCount:    q.ackedCount,
		InflightCount: q.inFlightManager.Size(),
		ProducedCount: q.producedCount,
		DlqCount:      q.deadLetterQueue.Size(),
	}
	return metrics
}

func (q *InMemoryBroker) processExpiredMessages() {
	q.mu.Lock()
	defer q.mu.Unlock()
	removeMessage := q.inFlightManager.Expired(q.config.AckTimeout)

	for _, item := range removeMessage {
		if !q.closed && q.queue.Size() == q.queue.Capacity() {
			continue // if broker full, skip
		}
		if item.Message.Retry >= q.config.MaxRetries {
			q.deadLetterQueue.Push(item.Message)
			q.inFlightManager.Remove(item.Token)
			continue
		}
		// repush
		msg := item.Message
		msg.Retry++
		q.queue.Push(msg)
		q.condCons.Signal()
		// delete
		q.inFlightManager.Remove(item.Token)
	}

}
