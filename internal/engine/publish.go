package engine

import (
	"fmt"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/google/uuid"
)

func (q *InMemoryBroker) Publish(message model.Message) error {
	q.mu.Lock()
	for !q.closed && q.queue.Size() == q.queue.Capacity() {
		q.condProd.Wait()
	}
	if q.closed {
		q.mu.Unlock()
		return ErrBrokerClosed
	}
	q.mu.Unlock()
	if message.ID == "" {
		message.ID = uuid.NewString()
	}
	message.Timestamp = time.Now().UTC()
	if err := q.wal.Append(message); err != nil {
		return err
	}

	q.mu.Lock()
	q.queue.Push(message)
	q.producedCount++
	q.mu.Unlock()

	select {
	case q.notifyDistributor <- struct{}{}:
	default:
	}

	return nil
}

func (q *InMemoryBroker) Ack(token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if ok := q.inFlightManager.IsPresent(token); !ok {
		return fmt.Errorf("Message not present in inflight, Ack token: %s", token)
	}
	q.inFlightManager.Remove(token)
	q.ackedCount++
	return nil
}

func (q *InMemoryBroker) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.condProd.Broadcast()
}

func (q *InMemoryBroker) Metrics() model.Metric {
	q.mu.Lock()
	defer q.mu.Unlock()
	metrics := model.Metric{
		AckedCount:           q.ackedCount,
		InflightCount:        q.inFlightManager.Size(),
		ProducedCount:        q.producedCount,
		DlqCount:             q.deadLetterQueue.Size(),
		RedeliveredCount:     q.redeliveredCount,
		ConsumerSessionCount: len(q.consumerSessions),
		QueueDepth:           q.queue.Size(),
	}
	return metrics
}
