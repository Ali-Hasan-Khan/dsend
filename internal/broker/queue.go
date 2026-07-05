package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Ali-Hasan-Khan/dsend/internal/dlq"
	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

var ErrBrokerClosed = errors.New("broker closed")

type Queue interface {
	Push(model.Message)
	Pop() model.Message
	Peek() model.Message
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

	inFlightManager *inflight.Manager
	deadLetterQueue DeadLetterQueue

	consumerSessions map[string]*session.ConsumerSession
	consumerOrder    []string
	nextConsumer     int

	notifyDistributor chan struct{}

	ackedCount       int
	producedCount    int
	redeliveredCount int

	wal storage.WAL

	config Config
}

func NewInMemoryBroker(cfg Config, messages []model.Message, wal storage.WAL) *InMemoryBroker {
	cap := max(cfg.QueueSize, len(messages))
	broker := &InMemoryBroker{
		queue:             queue.NewRingBufferQueue(cap),
		inFlightManager:   inflight.NewManager(),
		deadLetterQueue:   dlq.NewDLQ(),
		consumerSessions:  make(map[string]*session.ConsumerSession),
		consumerOrder:     make([]string, 0),
		notifyDistributor: make(chan struct{}, 1),
		wal:               wal,
		config:            cfg,
	}

	broker.condProd = sync.NewCond(&broker.mu)

	for _, msg := range messages {
		broker.queue.Push(msg)
		broker.producedCount++
	}
	broker.notifyDistributor <- struct{}{}
	return broker
}

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
					q.mu.Lock()
					id := q.consumerOrder[q.nextConsumer]
					session := q.consumerSessions[id]
					q.nextConsumer++
					q.nextConsumer %= len(q.consumerOrder)
					q.mu.Unlock()
					select {
					case <-session.Closed:
						continue
					case session.Deliveries <- delivery:
						q.popForDelivery(delivery.AckToken)
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

func (q *InMemoryBroker) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.condProd.Broadcast()
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
		q.redeliveredCount++
		// delete
		q.inFlightManager.Remove(item.Token)
	}

}
