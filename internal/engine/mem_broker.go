package engine

import (
	"errors"
	"sync"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
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

type InFlightManager interface {
	Add(token string, msg model.Message)
	Remove(token string)
	IsPresent(token string) bool
	Size() int
	Expired(timeout time.Duration) []model.Delivery
}

type InMemoryBroker struct {
	queue Queue
	mu    sync.Mutex

	closed bool

	condProd *sync.Cond

	inFlightManager InFlightManager
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

func NewInMemoryBroker(
	cfg Config,
	messages []model.Message,
	wal storage.WAL,
	q Queue,
	dlq DeadLetterQueue,
	inflight InFlightManager,
) *InMemoryBroker {
	broker := &InMemoryBroker{
		queue:             q,
		deadLetterQueue:   dlq,
		inFlightManager:   inflight,
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
