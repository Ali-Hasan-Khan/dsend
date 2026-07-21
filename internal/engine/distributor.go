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
				sess, delivery, ok := q.reserveDelivery()

				if !ok {
					break
				}

				select {
				case <-sess.Closed:
					q.cancelReservation(delivery)
				case sess.Deliveries <- delivery:
				default:
					q.cancelReservation(delivery)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (q *InMemoryBroker) reserveDelivery() (*session.ConsumerSession, model.Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queue.Size() == 0 || len(q.consumerSessions) == 0 {
		return nil, model.Delivery{}, false
	}

	session, ok := q.nextSession()
	if !ok {
		return nil, model.Delivery{}, false
	}

	token := uuid.NewString()
	message := q.queue.Pop()
	q.inFlightManager.Add(token, message)
	q.condProd.Signal()
	delivery := model.Delivery{
		Message:  message,
		AckToken: token,
	}

	return session, delivery, true
}

func (q *InMemoryBroker) nextSession() (*session.ConsumerSession, bool) {
	if len(q.consumerOrder) == 0 {
		return nil, false
	}

	if q.nextConsumer >= len(q.consumerOrder) {
		q.nextConsumer = 0
	}

	id := q.consumerOrder[q.nextConsumer]
	q.nextConsumer = (q.nextConsumer + 1) % len(q.consumerOrder)

	sess, ok := q.consumerSessions[id]
	if !ok {
		return nil, false
	}

	return sess, true
}

func (q *InMemoryBroker) cancelReservation(delivery model.Delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.inFlightManager.Remove(delivery.AckToken)
	q.queue.Push(delivery.Message)
	q.condProd.Signal()

	select {
	case q.notifyDistributor <- struct{}{}:
	default:
	}
}
