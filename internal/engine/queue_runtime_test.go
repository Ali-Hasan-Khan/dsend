package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

type mockWAL struct {
	mu        sync.Mutex
	appendErr error
	records   []model.Record
}

func (m *mockWAL) Append(record model.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.appendErr != nil {
		return m.appendErr
	}
	m.records = append(m.records, record)
	return nil
}

func (m *mockWAL) Load() (storage.RecoveredState, error) {
	return storage.RecoveredState{}, nil
}

func newTestBroker(wal *mockWAL) *QueueRuntime {
	cfg := DefaultConfig()
	cfg.QueueSize = 10

	return NewQueueRuntime(
		model.DefaultQueueName,
		cfg,
		nil,
		wal,
		queue.NewRingBufferQueue(cfg.QueueSize),
		queue.NewDLQ(),
		inflight.NewManager(),
	)
}

func TestPublish(t *testing.T) {
	tests := []struct {
		name       string
		message    model.Message
		expectedID string
	}{
		{
			name:    "empty payload",
			message: model.Message{},
		},
		{
			name: "normal payload",
			message: model.Message{
				Payload: "hello",
			},
		},
		{
			name: "existing id preserved",
			message: model.Message{
				ID:      "custom-id",
				Payload: "hello",
			},
			expectedID: "custom-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wal := &mockWAL{}
			b := newTestBroker(wal)

			err := b.Publish(tt.message)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			metrics := b.Metrics()

			if metrics.ProducedCount != 1 {
				t.Fatalf("expected produced count 1, got %d", metrics.ProducedCount)
			}

			if metrics.QueueDepth != 1 {
				t.Fatalf("expected queue depth 1, got %d", metrics.QueueDepth)
			}

			if len(wal.records) != 1 {
				t.Fatalf("expected WAL append")
			}

			msg := wal.records[0]

			if msg.Message.Timestamp.IsZero() {
				t.Fatal("timestamp should be populated")
			}

			if tt.expectedID != "" {
				if msg.MessageID != tt.expectedID {
					t.Fatalf("expected ID %q got %q", tt.expectedID, msg.MessageID)
				}
			} else if msg.MessageID == "" {
				t.Fatal("expected generated UUID")
			}
		})
	}
}

func TestPublishWALFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "append fails",
			err:  errors.New("wal append failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wal := &mockWAL{
				appendErr: tt.err,
			}

			b := newTestBroker(wal)

			err := b.Publish(model.Message{
				Payload: "hello",
			})

			if !errors.Is(err, tt.err) {
				t.Fatalf("expected %v got %v", tt.err, err)
			}

			metrics := b.Metrics()

			if metrics.ProducedCount != 0 {
				t.Fatal("message should not be published if WAL append fails")
			}

			if metrics.QueueDepth != 0 {
				t.Fatal("queue should remain empty")
			}
		})
	}
}

func TestPublishSetsCurrentTimestamp(t *testing.T) {
	wal := &mockWAL{}
	b := newTestBroker(wal)

	before := time.Now()

	err := b.Publish(model.Message{
		Payload: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	after := time.Now()

	ts := wal.records[0].Message.Timestamp

	if ts.Before(before) || ts.After(after) {
		t.Fatalf("timestamp %v not between %v and %v", ts, before, after)
	}
}

func TestBrokerRecoveryFromMessages(t *testing.T) {
	msgs := []model.Message{
		{
			ID:      "1",
			Payload: "one",
		},
		{
			ID:      "2",
			Payload: "two",
		},
		{
			ID:      "3",
			Payload: "three",
		},
	}

	cfg := DefaultConfig()

	b := NewQueueRuntime(
		model.DefaultQueueName,
		cfg,
		msgs,
		&mockWAL{},
		queue.NewRingBufferQueue(10),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	metrics := b.Metrics()

	if metrics.ProducedCount != 3 {
		t.Fatalf("expected produced count 3 got %d", metrics.ProducedCount)
	}

	if metrics.QueueDepth != 3 {
		t.Fatalf("expected queue depth 3 got %d", metrics.QueueDepth)
	}
}

func TestSubscribeReceivesPublishedMessages(t *testing.T) {
	broker := newIntegrationBroker(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	consumer := session.NewConsumerSession("consumer-1")
	broker.Subscribe(consumer)

	err := broker.Publish(model.Message{
		ID:      "1",
		Payload: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {

	case delivery := <-consumer.Deliveries:

		if delivery.ID != "1" {
			t.Fatalf("expected id=1 got=%s", delivery.ID)
		}

		if delivery.Payload != "hello" {
			t.Fatalf("expected payload=hello got=%s", delivery.Payload)
		}

	case <-time.After(time.Second):
		t.Fatal("consumer did not receive message")
	}

	metrics := broker.Metrics()

	if metrics.ConsumerSessionCount != 1 {
		t.Fatalf("expected 1 consumer got %d", metrics.ConsumerSessionCount)
	}
}

func TestUnsubscribeStopsDeliveries(t *testing.T) {
	broker := newIntegrationBroker(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	consumer := session.NewConsumerSession("consumer")

	broker.Subscribe(consumer)

	broker.Unsubscribe(consumer.ID)

	err := broker.Publish(model.Message{
		ID: "1",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {

	case <-consumer.Deliveries:
		t.Fatal("received delivery after unsubscribe")

	case <-time.After(300 * time.Millisecond):
	}

	if broker.Metrics().ConsumerSessionCount != 0 {
		t.Fatal("consumer still registered")
	}
}

func TestDoubleUnsubscribeIsSafe(t *testing.T) {
	broker := newIntegrationBroker(10)

	consumer := session.NewConsumerSession("consumer")

	broker.Subscribe(consumer)

	broker.Unsubscribe(consumer.ID)

	// should not panic
	broker.Unsubscribe(consumer.ID)

	if broker.Metrics().ConsumerSessionCount != 0 {
		t.Fatal("expected zero consumers")
	}
}

func TestMultipleSubscribersRegistered(t *testing.T) {
	broker := newIntegrationBroker(10)

	c1 := session.NewConsumerSession("1")
	c2 := session.NewConsumerSession("2")
	c3 := session.NewConsumerSession("3")

	broker.Subscribe(c1)
	broker.Subscribe(c2)
	broker.Subscribe(c3)

	if broker.Metrics().ConsumerSessionCount != 3 {
		t.Fatalf(
			"expected 3 consumers got %d",
			broker.Metrics().ConsumerSessionCount,
		)
	}
}

func TestUnsubscribeClosesSession(t *testing.T) {
	broker := newIntegrationBroker(10)

	consumer := session.NewConsumerSession("consumer")

	broker.Subscribe(consumer)

	broker.Unsubscribe(consumer.ID)

	select {

	case <-consumer.Closed:

	default:
		t.Fatal("session was not closed")
	}
}

func TestRoundRobinAfterConsumerLeaves(t *testing.T) {
	const totalMessages = 100

	broker := newIntegrationBroker(200)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	c1 := session.NewConsumerSession("c1")
	c2 := session.NewConsumerSession("c2")
	c3 := session.NewConsumerSession("c3")

	broker.Subscribe(c1)
	broker.Subscribe(c2)
	broker.Subscribe(c3)

	// Remove middle consumer.
	broker.Unsubscribe(c2.ID)

	var c1Count atomic.Int64
	var c2Count atomic.Int64
	var c3Count atomic.Int64

	var wg sync.WaitGroup
	wg.Add(2)

	startConsumer := func(s *session.ConsumerSession, count *atomic.Int64) {
		go func() {
			defer wg.Done()

			for {
				select {

				case d := <-s.Deliveries:
					count.Add(1)

					if err := broker.Ack(d.AckToken); err != nil {
						t.Errorf("ack failed: %v", err)
						return
					}

				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		}()
	}

	startConsumer(c1, &c1Count)
	startConsumer(c3, &c3Count)

	for i := 0; i < totalMessages; i++ {
		if err := broker.Publish(model.Message{
			ID:      fmt.Sprintf("%d", i),
			Payload: fmt.Sprintf("msg-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()

	// Ensure removed consumer never receives anything.
	select {

	case <-c2.Deliveries:
		c2Count.Add(1)

	default:
	}

	if c2Count.Load() != 0 {
		t.Fatalf("unsubscribed consumer received %d messages", c2Count.Load())
	}

	total := c1Count.Load() + c3Count.Load()

	if total != totalMessages {
		t.Fatalf(
			"expected %d total deliveries got %d",
			totalMessages,
			total,
		)
	}

	// Distribution should stay roughly even.
	if c1Count.Load() < 40 || c1Count.Load() > 60 {
		t.Fatalf("consumer1 received %d messages", c1Count.Load())
	}

	if c3Count.Load() < 40 || c3Count.Load() > 60 {
		t.Fatalf("consumer3 received %d messages", c3Count.Load())
	}

	waitForBrokerIdle(t, broker)

	metrics := broker.Metrics()

	if metrics.ConsumerSessionCount != 2 {
		t.Fatalf(
			"expected 2 consumers got %d",
			metrics.ConsumerSessionCount,
		)
	}

	if metrics.AckedCount != totalMessages {
		t.Fatalf(
			"expected %d acked got %d",
			totalMessages,
			metrics.AckedCount,
		)
	}

	if metrics.QueueDepth != 0 {
		t.Fatalf("expected empty queue got %d", metrics.QueueDepth)
	}

	if metrics.InflightCount != 0 {
		t.Fatalf("expected empty inflight got %d", metrics.InflightCount)
	}
}

func TestRecoveredMessagesCanBeConsumed(t *testing.T) {
	msgs := []model.Message{
		{ID: "1", Payload: "one"},
		{ID: "2", Payload: "two"},
		{ID: "3", Payload: "three"},
		{ID: "4", Payload: "four"},
		{ID: "5", Payload: "five"},
	}

	broker := NewQueueRuntime(
		model.DefaultQueueName,
		DefaultConfig(),
		msgs,
		&mockWAL{},
		queue.NewRingBufferQueue(10),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	consumer := session.NewConsumerSession("consumer")

	broker.Subscribe(consumer)

	for i := range msgs {

		select {

		case d := <-consumer.Deliveries:

			if d.ID != msgs[i].ID {
				t.Fatalf(
					"expected %s got %s",
					msgs[i].ID,
					d.ID,
				)
			}

			if err := broker.Ack(d.AckToken); err != nil {
				t.Fatal(err)
			}

		case <-time.After(time.Second):
			t.Fatal("timed out waiting for recovered message")
		}
	}

	waitForBrokerIdle(t, broker)

	metrics := broker.Metrics()

	if metrics.QueueDepth != 0 {
		t.Fatal("queue should be empty")
	}

	if metrics.AckedCount != len(msgs) {
		t.Fatalf(
			"expected %d acked got %d",
			len(msgs),
			metrics.AckedCount,
		)
	}
}

func newRedeliveryBroker() *QueueRuntime {
	cfg := DefaultConfig()
	cfg.QueueSize = 10
	cfg.AckTimeout = 100 * time.Millisecond
	cfg.RedeliveryInterval = 20 * time.Millisecond
	cfg.MaxRetries = 3

	return NewQueueRuntime(
		model.DefaultQueueName,
		cfg,
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(cfg.QueueSize),
		queue.NewDLQ(),
		inflight.NewManager(),
	)
}

func TestAckPreventsRedelivery(t *testing.T) {
	broker := newRedeliveryBroker()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)
	go broker.StartRedeliveryWorker(ctx)

	consumer := session.NewConsumerSession("consumer")

	broker.Subscribe(consumer)

	err := broker.Publish(model.Message{
		ID:      "msg-1",
		Payload: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	var delivery model.Delivery

	select {
	case delivery = <-consumer.Deliveries:

	case <-time.After(time.Second):
		t.Fatal("message was not delivered")
	}

	if err := broker.Ack(delivery.AckToken); err != nil {
		t.Fatal(err)
	}

	select {

	case <-consumer.Deliveries:
		t.Fatal("received unexpected redelivery")

	case <-time.After(300 * time.Millisecond):
	}

	waitForBrokerIdle(t, broker)

	metrics := broker.Metrics()

	if metrics.RedeliveredCount != 0 {
		t.Fatalf("expected 0 redeliveries got %d", metrics.RedeliveredCount)
	}

	if metrics.AckedCount != 1 {
		t.Fatalf("expected acked=1 got=%d", metrics.AckedCount)
	}

	if metrics.QueueDepth != 0 {
		t.Fatalf("expected queue empty got=%d", metrics.QueueDepth)
	}

	if metrics.InflightCount != 0 {
		t.Fatalf("expected inflight empty got=%d", metrics.InflightCount)
	}
}

func TestMessageIsRedelivered(t *testing.T) {
	broker := newRedeliveryBroker()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)
	go broker.StartRedeliveryWorker(ctx)

	consumer := session.NewConsumerSession("consumer")

	broker.Subscribe(consumer)

	err := broker.Publish(model.Message{
		ID:      "msg-1",
		Payload: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	var first model.Delivery

	select {
	case first = <-consumer.Deliveries:

	case <-time.After(time.Second):
		t.Fatal("did not receive first delivery")
	}

	var second model.Delivery

	select {
	case second = <-consumer.Deliveries:

	case <-time.After(time.Second):
		t.Fatal("message was not redelivered")
	}

	if first.ID != second.ID {
		t.Fatal("redelivered different message")
	}

	if first.AckToken == second.AckToken {
		t.Fatal("expected new ack token")
	}

	if second.Retry != 1 {
		t.Fatalf("expected retry=1 got=%d", second.Retry)
	}

	if err := broker.Ack(second.AckToken); err != nil {
		t.Fatal(err)
	}

	waitForBrokerIdle(t, broker)

	metrics := broker.Metrics()

	if metrics.RedeliveredCount != 1 {
		t.Fatalf("expected 1 redelivery got=%d", metrics.RedeliveredCount)
	}

	if metrics.AckedCount != 1 {
		t.Fatalf("expected acked=1 got=%d", metrics.AckedCount)
	}

	if metrics.QueueDepth != 0 {
		t.Fatalf("expected queue empty got=%d", metrics.QueueDepth)
	}

	if metrics.InflightCount != 0 {
		t.Fatalf("expected inflight empty got=%d", metrics.InflightCount)
	}
}

func TestMessageMovesToDLQAfterMaxRetries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.QueueSize = 10
	cfg.AckTimeout = 100 * time.Millisecond
	cfg.RedeliveryInterval = 20 * time.Millisecond
	cfg.MaxRetries = 2

	dlq := queue.NewDLQ()

	broker := NewQueueRuntime(
		model.DefaultQueueName,
		cfg,
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(cfg.QueueSize),
		dlq,
		inflight.NewManager(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)
	go broker.StartRedeliveryWorker(ctx)

	consumer := session.NewConsumerSession("consumer")

	broker.Subscribe(consumer)

	err := broker.Publish(model.Message{
		ID:      "msg-1",
		Payload: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Retry = 0
	select {
	case <-consumer.Deliveries:

	case <-time.After(time.Second):
		t.Fatal("missing first delivery")
	}

	// Retry = 1
	select {
	case <-consumer.Deliveries:

	case <-time.After(time.Second):
		t.Fatal("missing second delivery")
	}

	// Retry = 2
	select {
	case <-consumer.Deliveries:

	case <-time.After(time.Second):
		t.Fatal("missing third delivery")
	}

	// Should now go to DLQ instead of being delivered again.
	select {

	case <-consumer.Deliveries:
		t.Fatal("message should have gone to DLQ")

	case <-time.After(300 * time.Millisecond):
	}

	metrics := broker.Metrics()

	if metrics.DlqCount != 1 {
		t.Fatalf("expected dlq size=1 got=%d", metrics.DlqCount)
	}

	if metrics.RedeliveredCount != 2 {
		t.Fatalf(
			"expected redelivered=2 got=%d",
			metrics.RedeliveredCount,
		)
	}

	if metrics.InflightCount != 0 {
		t.Fatalf(
			"expected inflight=0 got=%d",
			metrics.InflightCount,
		)
	}

	if metrics.QueueDepth != 0 {
		t.Fatalf(
			"expected queue empty got=%d",
			metrics.QueueDepth,
		)
	}

	msg := dlq.Peek()

	if msg.ID != "msg-1" {
		t.Fatalf(
			"expected msg-1 got=%s",
			msg.ID,
		)
	}

	if msg.Payload != "hello" {
		t.Fatalf(
			"expected payload hello got=%s",
			msg.Payload,
		)
	}

	if msg.Retry != cfg.MaxRetries {
		t.Fatalf(
			"expected retry=%d got=%d",
			cfg.MaxRetries,
			msg.Retry,
		)
	}
}

func TestShutdownRejectsNewPublishes(t *testing.T) {
	broker := newTestBroker(&mockWAL{})

	broker.Shutdown()

	err := broker.Publish(model.Message{
		ID: "1",
	})

	if !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("expected ErrBrokerClosed got %v", err)
	}
}

func TestShutdownUnblocksBlockedPublisher(t *testing.T) {
	cfg := DefaultConfig()
	cfg.QueueSize = 1

	broker := NewQueueRuntime(
		model.DefaultQueueName,
		cfg,
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(1),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	err := broker.Publish(model.Message{
		ID: "1",
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)

	go func() {
		errCh <- broker.Publish(model.Message{
			ID: "2",
		})
	}()

	time.Sleep(100 * time.Millisecond)

	broker.Shutdown()

	select {

	case err := <-errCh:

		if !errors.Is(err, ErrBrokerClosed) {
			t.Fatalf("expected ErrBrokerClosed got %v", err)
		}

	case <-time.After(time.Second):
		t.Fatal("blocked publisher never woke")
	}
}

func TestShutdownClosesAllConsumerSessions(t *testing.T) {
	broker := newIntegrationBroker(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	c1 := session.NewConsumerSession("c1")
	c2 := session.NewConsumerSession("c2")
	c3 := session.NewConsumerSession("c3")

	broker.Subscribe(c1)
	broker.Subscribe(c2)
	broker.Subscribe(c3)

	broker.Shutdown()

	for _, consumer := range []*session.ConsumerSession{
		c1,
		c2,
		c3,
	} {

		select {

		case <-consumer.Closed:

		default:
			t.Fatalf("consumer %s not closed", consumer.ID)
		}
	}

	if broker.Metrics().ConsumerSessionCount != 0 {
		t.Fatal("expected zero registered consumers")
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	broker := newTestBroker(&mockWAL{})

	broker.Shutdown()

	// Should not panic.
	broker.Shutdown()

	err := broker.Publish(model.Message{})

	if !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("expected ErrBrokerClosed got %v", err)
	}
}

func TestAck(t *testing.T) {
	tests := []struct {
		name         string
		token        string
		setup        func(*QueueRuntime)
		wantErr      bool
		wantAcked    int
		wantInflight int
	}{
		{
			name:  "valid ack",
			token: "token-1",
			setup: func(b *QueueRuntime) {
				b.inFlightManager.Add("token-1", model.Message{
					ID: "msg-1",
				})
			},
			wantAcked:    1,
			wantInflight: 0,
		},
		{
			name:         "invalid token",
			token:        "missing",
			setup:        func(*QueueRuntime) {},
			wantErr:      true,
			wantAcked:    0,
			wantInflight: 0,
		},
		{
			name:  "duplicate ack",
			token: "token-2",
			setup: func(b *QueueRuntime) {
				b.inFlightManager.Add("token-2", model.Message{
					ID: "msg-2",
				})

				if err := b.Ack("token-2"); err != nil {
					t.Fatalf("unexpected setup error: %v", err)
				}
			},
			wantErr:      true,
			wantAcked:    1,
			wantInflight: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			wal := &mockWAL{}

			b := NewQueueRuntime(
				model.DefaultQueueName,
				DefaultConfig(),
				nil,
				wal,
				queue.NewRingBufferQueue(10),
				queue.NewDLQ(),
				inflight.NewManager(),
			)

			tt.setup(b)

			err := b.Ack(tt.token)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}

				if !errors.Is(err, ErrInvalidAckToken) {
					t.Fatalf("unexpected error: %v", err)
				}

			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			metrics := b.Metrics()

			if metrics.AckedCount != tt.wantAcked {
				t.Fatalf(
					"expected ack count %d got %d",
					tt.wantAcked,
					metrics.AckedCount,
				)
			}

			if metrics.InflightCount != tt.wantInflight {
				t.Fatalf(
					"expected inflight %d got %d",
					tt.wantInflight,
					metrics.InflightCount,
				)
			}
		})
	}
}

func TestAckDoesNotChangeProducedCount(t *testing.T) {
	b := NewQueueRuntime(
		model.DefaultQueueName,
		DefaultConfig(),
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(10),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	b.producedCount = 5

	b.inFlightManager.Add("token", model.Message{})

	if err := b.Ack("token"); err != nil {
		t.Fatal(err)
	}

	if got := b.Metrics().ProducedCount; got != 5 {
		t.Fatalf("expected produced count 5 got %d", got)
	}
}

func TestAckDoesNotChangeQueueDepth(t *testing.T) {
	b := NewQueueRuntime(
		model.DefaultQueueName,
		DefaultConfig(),
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(10),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	b.queue.Push(model.Message{ID: "1"})
	b.queue.Push(model.Message{ID: "2"})

	b.inFlightManager.Add("token", model.Message{})

	before := b.Metrics().QueueDepth

	if err := b.Ack("token"); err != nil {
		t.Fatal(err)
	}

	after := b.Metrics().QueueDepth

	if before != after {
		t.Fatalf(
			"queue depth changed from %d to %d",
			before,
			after,
		)
	}
}
