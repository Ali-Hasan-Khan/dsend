package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	timeout    int = 1000 // in ms
	maxRetries int = 3
)

type Queue struct {
	readyQueue []Message // ready queue
	cap        int
	size       int
	head       int
	tail       int
	closed     bool
	mu         sync.Mutex

	condProd *sync.Cond
	condCons *sync.Cond

	inFlight        map[string]InFlightMessage // inflight hashmap
	deadLetterQueue []Message                  // dead letter queue

	ackedCount    int
	producedCount int
	dlqCount      int
}

func (q *Queue) IsDone() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	done := q.ackedCount+q.dlqCount == q.producedCount
	return done
}

func (q *Queue) StartRedeliveryLoop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	removeMessage := make([]ExpiredDelivery, 0)
	for idx, item := range q.inFlight {
		if time.Since(item.DeliveredAt) > time.Millisecond*time.Duration(timeout) {
			removeMessage = append(removeMessage, ExpiredDelivery{
				token:   idx,
				message: item.Message,
			})
		}
	}

	for _, item := range removeMessage {
		if !q.closed && q.size == q.cap {
			continue // if broker full, skip
		}
		if item.message.Retry >= maxRetries {
			q.deadLetterQueue = append(q.deadLetterQueue, item.message)
			q.dlqCount++
			delete(q.inFlight, item.token)
			continue
		}
		// repush
		msg := item.message
		msg.Retry++
		q.enqueue(msg)
		q.condCons.Signal()
		// delete
		delete(q.inFlight, item.token)
	}

}

func (q *Queue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.condProd.Broadcast()
	q.condCons.Broadcast()
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

func (q *Queue) Push(message Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.size == q.cap {
		q.condProd.Wait()
	}
	if q.closed {
		return
	}
	q.enqueue(message)
	q.producedCount++
	q.condCons.Signal()
}

func (q *Queue) Pop() (Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.size == 0 {
		q.condCons.Wait()
	}
	if q.closed && q.size == 0 {
		return Delivery{}, false
	}
	message := q.readyQueue[q.head]
	token := uuid.NewString()
	q.dequeue()
	q.inFlight[token] = InFlightMessage{
		Message:     message,
		DeliveredAt: time.Now(),
	}
	q.condProd.Signal()
	delivery := Delivery{
		Message:  message,
		AckToken: token,
	}
	return delivery, true
}

func (q *Queue) Ack(token string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.inFlight[token]; !ok {
		return // for now ignore invalid messageID
	}
	delete(q.inFlight, token)
	q.ackedCount++
}

func (q *Queue) GetMetrics() Metric {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Metric{
		ProducedCount: q.producedCount,
		AckedCount:    q.ackedCount,
		DlqCount:      q.dlqCount,
		InflightCount: len(q.inFlight),
	}
}

func (q *Queue) enqueue(message Message) {
	q.readyQueue[q.tail] = message
	q.tail = (q.tail + 1) % q.cap
	q.size++
}

func (q *Queue) dequeue() {
	q.readyQueue[q.head] = Message{}
	q.head = (q.head + 1) % q.cap
	q.size--
}

func (q *Queue) String() string {
	return fmt.Sprint(q.readyQueue)
}

func (q *Queue) GetDLQSize() int {
	return len(q.deadLetterQueue)
}
