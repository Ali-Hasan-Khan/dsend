package engine

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

var (
	ErrQueueExists     = errors.New("queue already exists")
	ErrQueueNotEmpty   = errors.New("queue is not empty")
	ErrQueueNotFound   = errors.New("queue does not exist")
	ErrInvalidQueue    = errors.New("queue name is invalid")
	ErrBrokerClosed    = errors.New("broker closed")
	ErrInvalidAckToken = errors.New("invalid ack token")
)

type InMemoryBroker struct {
	mu      sync.Mutex
	tokenMu sync.Mutex
	started bool
	closed  bool
	cfg     Config
	wal     storage.WAL

	queues      map[string]*QueueRuntime
	tokenOwners map[string]*QueueRuntime

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewInMemoryBroker(
	cfg Config,
	wal storage.WAL,
	queues map[string]*QueueRuntime,
	tokenOwners map[string]*QueueRuntime,
) *InMemoryBroker {
	broker := &InMemoryBroker{
		cfg:         cfg,
		wal:         wal,
		queues:      queues,
		tokenOwners: tokenOwners,
	}

	return broker
}

func (b *InMemoryBroker) newQueueRuntime(
	name string,
	messages []model.Message,
) *QueueRuntime {
	runtime := NewQueueRuntime(
		name,
		b.cfg,
		messages,
		b.wal,
		queue.NewRingBufferQueue(max(b.cfg.QueueSize, len(messages))),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	runtime.SetTokenHooks(
		func(token string) {
			b.tokenMu.Lock()
			defer b.tokenMu.Unlock()
			b.tokenOwners[token] = runtime
		},
		func(token string) {
			b.tokenMu.Lock()
			defer b.tokenMu.Unlock()
			delete(b.tokenOwners, token)
		},
	)

	return runtime
}

func validateQueueName(name string) error {
	if name == "" || len(name) > 255 {
		return ErrInvalidQueue
	}
	return nil
}

func (b *InMemoryBroker) startQueue(runtime *QueueRuntime) {
	b.wg.Add(2)

	go func() {
		defer b.wg.Done()
		runtime.RunDistributor(b.ctx)
	}()

	go func() {
		defer b.wg.Done()
		runtime.StartRedeliveryWorker(b.ctx)
	}()
}

func (b *InMemoryBroker) CreateQueue(name string) error {
	if err := validateQueueName(name); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}
	if _, exists := b.queues[name]; exists {
		return ErrQueueExists
	}

	if err := b.wal.Append(model.Record{
		Type:  model.QueueCreated,
		Queue: name,
	}); err != nil {
		return err
	}

	runtime := b.newQueueRuntime(name, nil)

	b.queues[name] = runtime
	if b.started {
		b.startQueue(runtime)
	}

	return nil
}

func (b *InMemoryBroker) DeleteQueue(name string) error {
	if name == model.DefaultQueueName {
		return ErrInvalidQueue
	}
	b.mu.Lock()
	runtime, ok := b.queues[name]
	if !ok {
		b.mu.Unlock()
		return ErrQueueNotFound
	}

	if !runtime.CloseIfEmpty() {
		b.mu.Unlock()
		return ErrQueueNotEmpty
	}

	if err := b.wal.Append(model.Record{
		Type:  model.QueueDeleted,
		Queue: name,
	}); err != nil {
		b.mu.Unlock()
		return err
	}

	delete(b.queues, name)
	b.mu.Unlock()
	runtime.Shutdown()

	b.tokenMu.Lock()
	for token, owner := range b.tokenOwners {
		if owner == runtime {
			delete(b.tokenOwners, token)
		}
	}
	b.tokenMu.Unlock()

	return nil
}

func (b *InMemoryBroker) ListQueues() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	names := make([]string, 0, len(b.queues))
	for name := range b.queues {
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}

func (b *InMemoryBroker) queue(name string) (*QueueRuntime, error) {
	if name == "" {
		name = model.DefaultQueueName
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	runtime, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	return runtime, nil
}

func (b *InMemoryBroker) Publish(queueName string, message model.Message) error {
	runtime, err := b.queue(queueName)
	if err != nil {
		return err
	}

	return runtime.Publish(message)
}

func (b *InMemoryBroker) Subscribe(
	queueName string,
	session *session.ConsumerSession,
) error {
	runtime, err := b.queue(queueName)
	if err != nil {
		return err
	}

	if err := runtime.Subscribe(session); err != nil {
		return err
	}
	return nil
}

func (b *InMemoryBroker) Ack(token string) error {
	b.tokenMu.Lock()
	runtime, ok := b.tokenOwners[token]
	b.tokenMu.Unlock()

	if !ok {
		return ErrInvalidAckToken
	}

	return runtime.Ack(token)
}

func (b *InMemoryBroker) Start(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.started || b.closed {
		return
	}

	b.ctx, b.cancel = context.WithCancel(ctx)
	b.started = true
	for _, runtime := range b.queues {
		b.startQueue(runtime)
	}
}

func (b *InMemoryBroker) Shutdown() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	runtimes := make([]*QueueRuntime, 0, len(b.queues))
	for _, runtime := range b.queues {
		runtimes = append(runtimes, runtime)
	}
	cancel := b.cancel
	b.mu.Unlock()

	for _, runtime := range runtimes {
		runtime.Shutdown()
	}
	if cancel != nil {
		cancel()
	}

	b.tokenMu.Lock()
	clear(b.tokenOwners)
	b.tokenMu.Unlock()
	b.wg.Wait()
}

func (b *InMemoryBroker) Metrics() model.BrokerMetrics {
	b.mu.Lock()
	runtimes := make(map[string]*QueueRuntime, len(b.queues))
	maps.Copy(runtimes, b.queues)
	b.mu.Unlock()

	result := model.BrokerMetrics{
		Queues: make([]model.QueueMetric, 0, len(runtimes)),
	}

	for name, runtime := range runtimes {
		metric := runtime.Metrics()
		result.Queues = append(result.Queues, model.QueueMetric{
			Name:   name,
			Metric: metric,
		})

		result.Total.ProducedCount += metric.ProducedCount
		result.Total.AckedCount += metric.AckedCount
		result.Total.DlqCount += metric.DlqCount
		result.Total.InflightCount += metric.InflightCount
		result.Total.RedeliveredCount += metric.RedeliveredCount
		result.Total.ConsumerSessionCount += metric.ConsumerSessionCount
		result.Total.QueueDepth += metric.QueueDepth
	}

	slices.SortFunc(result.Queues, func(a, b model.QueueMetric) int {
		return strings.Compare(a.Name, b.Name)
	})

	return result
}

func (b *InMemoryBroker) Unsubscribe(queueName, sessionID string) {
	runtime, err := b.queue(queueName)
	if err != nil {
		return
	}

	runtime.Unsubscribe(sessionID)
}

func (b *InMemoryBroker) QueueMetrics(queueName string) (model.Metric, error) {
	runtime, err := b.queue(queueName)
	if err != nil {
		return model.Metric{}, err
	}

	return runtime.Metrics(), nil
}
