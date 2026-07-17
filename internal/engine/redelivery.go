package engine

import (
	"context"
	"time"
)

func (q *InMemoryBroker) StartRedeliveryWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			q.processExpiredMessages()
			select {
			case q.notifyDistributor <- struct{}{}:
			default:
			}
		case <-ctx.Done():
			return
		}
	}
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
			q.inFlightManager.Remove(item.AckToken)
			continue
		}
		// repush
		msg := item.Message
		msg.Retry++
		q.queue.Push(msg)
		q.redeliveredCount++
		// delete
		q.inFlightManager.Remove(item.AckToken)
	}

}
