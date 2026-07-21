package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

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

	broker := NewInMemoryBroker(
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
