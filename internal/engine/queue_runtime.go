package engine

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
	"github.com/google/uuid"
)

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
	Peek() model.Message
}

type InFlightManager interface {
	Add(token string, msg model.Message, sessionID string)
	Remove(token string) (model.InFlightMessage, bool)
	Get(token string) (model.InFlightMessage, bool)
	IsPresent(token string) bool
	Size() int
	Expired(timeout time.Duration) []model.Delivery
}

type QueueRuntime struct {
	name   string
	wal    storage.WAL
	config Config

	mu       sync.Mutex
	condProd *sync.Cond
	closed   bool

	queue             Queue
	inFlightManager   InFlightManager
	deadLetterQueue   DeadLetterQueue
	consumerSessions  map[string]*session.ConsumerSession
	consumerOrder     []string
	nextConsumer      int
	notifyDistributor chan struct{}

	registerToken func(string)
	removeToken   func(string)

	ackedCount        int
	producedCount     int
	redeliveredCount  int
	deadletteredCount int
}

func NewQueueRuntime(
	name string,
	cfg Config,
	messages []model.Message,
	wal storage.WAL,
	q Queue,
	dlq DeadLetterQueue,
	manager InFlightManager,
) *QueueRuntime {
	runtime := &QueueRuntime{
		name:              name,
		queue:             q,
		deadLetterQueue:   dlq,
		inFlightManager:   manager,
		consumerSessions:  make(map[string]*session.ConsumerSession),
		consumerOrder:     make([]string, 0),
		notifyDistributor: make(chan struct{}, 1),
		wal:               wal,
		config:            cfg,
	}

	runtime.condProd = sync.NewCond(&runtime.mu)

	for _, msg := range messages {
		runtime.queue.Push(msg)
		runtime.producedCount++
	}
	runtime.notifyDistributor <- struct{}{}
	return runtime
}

func (q *QueueRuntime) Publish(message model.Message, ttl time.Duration) error {
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
	if ttl > 0 {
		expiryAt := message.Timestamp.Add(ttl)
		message.ExpiryAt = &expiryAt
	}
	record := model.Record{
		Type:      model.Published,
		Queue:     q.name,
		Message:   message,
		MessageID: message.ID,
	}
	if err := q.wal.Append(record); err != nil {
		return err
	}

	q.queue.Push(message)
	q.producedCount++

	select {
	case q.notifyDistributor <- struct{}{}:
	default:
	}

	return nil
}

func (q *QueueRuntime) Ack(token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	inflightMessage, ok := q.inFlightManager.Get(token)
	if !ok {
		return ErrInvalidAckToken
	}

	if err := q.wal.Append(model.Record{
		Type:      model.Acknowledged,
		Queue:     q.name,
		Message:   inflightMessage.Message,
		MessageID: inflightMessage.ID,
	}); err != nil {
		return err
	}

	q.inFlightManager.Remove(token)
	if q.removeToken != nil {
		q.removeToken(token)
	}
	q.ackedCount++
	sess, exists := q.consumerSessions[inflightMessage.ConsumerID]
	if exists {
		sess.UnackedCount--
	}

	select {
	case q.notifyDistributor <- struct{}{}:
	default:
	}

	return nil
}

func (q *QueueRuntime) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true

	for _, s := range q.consumerSessions {
		s.Close()
	}

	q.consumerSessions = make(map[string]*session.ConsumerSession)
	q.consumerOrder = nil
	q.nextConsumer = 0

	q.condProd.Broadcast()
}

func (q *QueueRuntime) Metrics() model.Metric {
	q.mu.Lock()
	defer q.mu.Unlock()
	metrics := model.Metric{
		AckedCount:           q.ackedCount,
		InflightCount:        q.inFlightManager.Size(),
		ProducedCount:        q.producedCount,
		DlqCount:             q.deadletteredCount,
		RedeliveredCount:     q.redeliveredCount,
		ConsumerSessionCount: len(q.consumerSessions),
		QueueDepth:           q.queue.Size(),
	}
	return metrics
}

func (q *QueueRuntime) Subscribe(session *session.ConsumerSession) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrBrokerClosed
	}

	if _, exists := q.consumerSessions[session.ID]; exists {
		return nil
	}

	q.consumerSessions[session.ID] = session
	q.consumerOrder = append(q.consumerOrder, session.ID)

	select {
	case q.notifyDistributor <- struct{}{}:
	default:
	}

	return nil
}

func (q *QueueRuntime) Unsubscribe(id string) {
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

func (q *QueueRuntime) SetTokenHooks(register func(string), remove func(string)) {
	q.registerToken = register
	q.removeToken = remove
}

func (q *QueueRuntime) cancelReservation(delivery model.Delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, ok := q.inFlightManager.Remove(delivery.AckToken)
	if ok {
		sess, exists := q.consumerSessions[delivery.ConsumerID]
		if exists {
			sess.UnackedCount--
		}
	}

	if q.removeToken != nil {
		q.removeToken(delivery.AckToken)
	}
	q.queue.Push(delivery.Message)
	q.condProd.Signal()

	select {
	case q.notifyDistributor <- struct{}{}:
	default:
	}
}

func (q *QueueRuntime) nextSession() (*session.ConsumerSession, bool) {
	if len(q.consumerOrder) == 0 {
		return nil, false
	}

	if q.nextConsumer >= len(q.consumerOrder) {
		q.nextConsumer = 0
	}

	var id string
	var found bool
	currConsumer := q.nextConsumer
	for {
		id = q.consumerOrder[q.nextConsumer]
		q.nextConsumer = (q.nextConsumer + 1) % len(q.consumerOrder)
		if q.consumerSessions[id].IsEligible(q.config.ConsumerPrefetch) {
			found = true
			break
		}
		if q.nextConsumer == currConsumer {
			break
		}
	}

	if !found {
		return nil, false
	}

	sess, ok := q.consumerSessions[id]
	if !ok {
		return nil, false
	}

	return sess, true
}

func (q *QueueRuntime) reserveDelivery() (*session.ConsumerSession, model.Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.deadLetterExpiredMessages(); err != nil {
		return nil, model.Delivery{}, false
	}

	if q.queue.Size() == 0 || len(q.consumerSessions) == 0 {
		return nil, model.Delivery{}, false
	}

	session, ok := q.nextSession()
	if !ok {
		return nil, model.Delivery{}, false
	}

	token := uuid.NewString()
	message := q.queue.Pop()
	q.inFlightManager.Add(token, message, session.ID)
	session.UnackedCount++
	if q.registerToken != nil {
		q.registerToken(token)
	}
	q.condProd.Signal()
	delivery := model.Delivery{
		Message:    message,
		AckToken:   token,
		ConsumerID: session.ID,
	}

	return session, delivery, true
}

func (q *QueueRuntime) isExpired(message model.Message) bool {
	return message.ExpiryAt != nil && !time.Now().Before(*message.ExpiryAt)
}

func (q *QueueRuntime) deadLetterExpiredMessages() error {
	for q.queue.Size() > 0 {
		message := q.queue.Peek()
		if !q.isExpired(message) {
			break
		}

		if err := q.wal.Append(model.Record{
			Type:      model.DeadLettered,
			Queue:     q.name,
			MessageID: message.ID,
		}); err != nil {
			return err
		}

		q.queue.Pop()
		q.deadLetterQueue.Push(message)
		q.deadletteredCount++
		q.condProd.Signal()
	}

	return nil
}

func (q *QueueRuntime) checkExpiredMessages() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.deadLetterExpiredMessages()
}

func (q *QueueRuntime) RunDistributor(ctx context.Context) {
	for {
		q.mu.Lock()
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return
		}

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

func (q *QueueRuntime) processExpiredMessages() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	removeMessage := q.inFlightManager.Expired(q.config.AckTimeout)

	for _, item := range removeMessage {
		if q.queue.Size() == q.queue.Capacity() {
			continue // if broker full, skip
		}
		if item.Message.Retry >= q.config.MaxRetries {
			if err := q.wal.Append(model.Record{
				Type:      model.DeadLettered,
				Queue:     q.name,
				MessageID: item.Message.ID,
			}); err != nil {
				continue
			}

			q.deadLetterQueue.Push(item.Message)
			q.inFlightManager.Remove(item.AckToken)
			if q.removeToken != nil {
				q.removeToken(item.AckToken)
			}
			q.deadletteredCount++
			sess, exists := q.consumerSessions[item.ConsumerID]
			if exists {
				sess.UnackedCount--
			}
			continue
		}

		message := item.Message
		message.Retry++

		if err := q.wal.Append(model.Record{
			Type:      model.Requeued,
			Queue:     q.name,
			Message:   message,
			MessageID: message.ID,
		}); err != nil {
			continue
		}

		q.queue.Push(message)
		q.inFlightManager.Remove(item.AckToken)
		if q.removeToken != nil {
			q.removeToken(item.AckToken)
		}
		q.redeliveredCount++
		sess, exists := q.consumerSessions[item.ConsumerID]
		if exists {
			sess.UnackedCount--
		}

		select {
		case q.notifyDistributor <- struct{}{}:
		default:
		}
	}

}

func (q *QueueRuntime) StartInflightRedeliveryWorker(ctx context.Context) {
	ticker := time.NewTicker(q.config.RedeliveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			q.mu.Lock()
			closed := q.closed
			q.mu.Unlock()
			if closed {
				return
			}
			q.processExpiredMessages()
		case <-ctx.Done():
			return
		}
	}
}

func (q *QueueRuntime) StartExpiryWorker(ctx context.Context) {
	ticker := time.NewTicker(q.config.ExpiryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			q.mu.Lock()
			closed := q.closed
			q.mu.Unlock()
			if closed {
				return
			}
			_ = q.checkExpiredMessages()
		case <-ctx.Done():
			return
		}
	}
}

func (q *QueueRuntime) CloseIfEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.queue.Size() != 0 || q.inFlightManager.Size() != 0 {
		return false
	}

	q.closed = true
	for _, session := range q.consumerSessions {
		session.Close()
	}

	q.consumerSessions = make(map[string]*session.ConsumerSession)
	q.consumerOrder = nil
	q.nextConsumer = 0
	q.condProd.Broadcast()

	return true
}
