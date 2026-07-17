package engine

import (
	"context"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/google/uuid"
)

func (q *InMemoryBroker) RunDistributor(ctx context.Context) {
	for {
		select {
		case <-q.notifyDistributor:
			for {

				q.mu.Lock()
				if q.queue.Size() == 0 || len(q.consumerSessions) == 0 {
					q.mu.Unlock()
					break
				}
				q.mu.Unlock()

				delivery, ok := q.peek()
				if ok {
					session := q.nextSession()
					select {
					case <-session.Closed:
						continue
					case session.Deliveries <- delivery:
						q.popForDelivery(delivery.AckToken) // delivery successful
					case <-ctx.Done():
						return
					default: // consumer busy -> ignore
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (q *InMemoryBroker) nextSession() *session.ConsumerSession {
	q.mu.Lock()
	defer q.mu.Unlock()
	id := q.consumerOrder[q.nextConsumer]
	session := q.consumerSessions[id]
	q.nextConsumer++
	q.nextConsumer %= len(q.consumerOrder)
	return session
}

func (q *InMemoryBroker) popForDelivery(token string) (model.Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queue.Size() == 0 {
		return model.Delivery{}, false
	}
	message := q.queue.Pop()
	q.inFlightManager.Add(token, message)
	q.condProd.Signal()
	delivery := model.Delivery{
		Message:  message,
		AckToken: token,
	}
	return delivery, true
}

func (q *InMemoryBroker) peek() (model.Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queue.Size() == 0 {
		return model.Delivery{}, false
	}
	token := uuid.NewString()
	message := q.queue.Peek()
	delivery := model.Delivery{
		Message:  message,
		AckToken: token,
	}
	return delivery, true
}
