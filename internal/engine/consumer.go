package engine

import (
	"slices"

	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

func (q *InMemoryBroker) Subscribe(session *session.ConsumerSession) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.consumerSessions[session.ID] = session
	q.consumerOrder = append(q.consumerOrder, session.ID)
	select {
	case q.notifyDistributor <- struct{}{}:
	default:
	}
}

func (q *InMemoryBroker) Unsubscribe(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.consumerSessions[id]; !ok {
		return
	}
	sess := q.consumerSessions[id]
	delete(q.consumerSessions, id)
	q.consumerOrder = slices.DeleteFunc(q.consumerOrder, func(sessionId string) bool {
		return sessionId == id
	})
	if len(q.consumerOrder) == 0 {
		q.nextConsumer = 0
	} else {
		q.nextConsumer %= len(q.consumerOrder)
	}
	sess.Close()
}
