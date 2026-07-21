package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

func newRedeliveryBroker() *InMemoryBroker {
	cfg := DefaultConfig()
	cfg.QueueSize = 10
	cfg.AckTimeout = 100 * time.Millisecond
	cfg.RedeliveryInterval = 20 * time.Millisecond
	cfg.MaxRetries = 3

	return NewInMemoryBroker(
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

	broker := NewInMemoryBroker(
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
