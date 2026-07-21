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

func newIntegrationBroker(queueSize int) *InMemoryBroker {
	cfg := DefaultConfig()
	cfg.QueueSize = queueSize
	cfg.AckTimeout = time.Second

	return NewInMemoryBroker(
		cfg,
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(queueSize),
		queue.NewDLQ(),
		inflight.NewManager(),
	)
}

func waitForBrokerIdle(t *testing.T, broker *InMemoryBroker) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		m := broker.Metrics()

		if m.QueueDepth == 0 && m.InflightCount == 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("broker did not become idle")
}

func TestBrokerPublishConsumeAck(t *testing.T) {
	const (
		producers           = 2
		messagesPerProducer = 50
		totalMessages       = producers * messagesPerProducer
	)

	broker := newIntegrationBroker(200)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	consumer := session.NewConsumerSession("consumer-1")
	broker.Subscribe(consumer)

	var producerWG sync.WaitGroup

	for p := 0; p < producers; p++ {
		producerWG.Add(1)

		go func(id int) {
			defer producerWG.Done()

			for i := 0; i < messagesPerProducer; i++ {
				err := broker.Publish(model.Message{
					Payload: fmt.Sprintf("producer-%d-msg-%d", id, i),
				})

				if err != nil {
					t.Errorf("publish failed: %v", err)
				}
			}
		}(p)
	}

	var consumerWG sync.WaitGroup
	consumerWG.Add(1)

	go func() {
		defer consumerWG.Done()

		for i := 0; i < totalMessages; i++ {
			select {

			case delivery := <-consumer.Deliveries:

				if err := broker.Ack(delivery.AckToken); err != nil {
					t.Errorf("ack failed: %v", err)
				}

			case <-time.After(5 * time.Second):
				t.Errorf("timed out waiting for delivery")
				return
			}
		}
	}()

	producerWG.Wait()
	consumerWG.Wait()

	waitForBrokerIdle(t, broker)

	metrics := broker.Metrics()

	if metrics.ProducedCount != totalMessages {
		t.Fatalf(
			"expected produced=%d got=%d",
			totalMessages,
			metrics.ProducedCount,
		)
	}

	if metrics.AckedCount != totalMessages {
		t.Fatalf(
			"expected acked=%d got=%d",
			totalMessages,
			metrics.AckedCount,
		)
	}

	if metrics.QueueDepth != 0 {
		t.Fatalf(
			"expected empty queue got=%d",
			metrics.QueueDepth,
		)
	}

	if metrics.InflightCount != 0 {
		t.Fatalf(
			"expected empty inflight got=%d",
			metrics.InflightCount,
		)
	}
}

func TestBrokerRoundRobinConsumers(t *testing.T) {
	const totalMessages = 300

	broker := newIntegrationBroker(500)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	c1 := session.NewConsumerSession("c1")
	c2 := session.NewConsumerSession("c2")
	c3 := session.NewConsumerSession("c3")

	broker.Subscribe(c1)
	broker.Subscribe(c2)
	broker.Subscribe(c3)

	var counts [3]atomic.Int64
	var consumed atomic.Int64

	var wg sync.WaitGroup
	wg.Add(3)

	startConsumer := func(idx int, s *session.ConsumerSession) {
		go func() {
			defer wg.Done()

			for {
				select {

				case d := <-s.Deliveries:
					counts[idx].Add(1)
					current := consumed.Add(1)

					if err := broker.Ack(d.AckToken); err != nil {
						t.Errorf("ack failed: %v", err)
						return
					}

					if current == totalMessages {
						return
					}

				case <-time.After(5 * time.Second):
					return
				}
			}
		}()
	}

	startConsumer(0, c1)
	startConsumer(1, c2)
	startConsumer(2, c3)

	for i := 0; i < totalMessages; i++ {
		if err := broker.Publish(model.Message{
			Payload: fmt.Sprintf("%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()

	total := counts[0].Load() + counts[1].Load() + counts[2].Load()

	if total != totalMessages {
		t.Fatalf(
			"expected %d consumed got %d",
			totalMessages,
			total,
		)
	}

	for i := range counts {
		c := counts[i].Load()
		if c < 90 || c > 110 {
			t.Fatalf(
				"consumer %d received %d messages (expected roughly 100)",
				i+1,
				c,
			)
		}
	}
}

func TestBrokerStress(t *testing.T) {
	const (
		producers     = 20
		consumers     = 10
		perProducer   = 500
		totalMessages = producers * perProducer
	)

	broker := newIntegrationBroker(totalMessages)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.RunDistributor(ctx)

	var consumerWG sync.WaitGroup

	var acked atomic.Int64

	for i := 0; i < consumers; i++ {

		s := session.NewConsumerSession(fmt.Sprintf("consumer-%d", i))

		broker.Subscribe(s)

		consumerWG.Add(1)

		go func(sess *session.ConsumerSession) {
			defer consumerWG.Done()

			for {

				if acked.Load() == totalMessages {
					return
				}

				select {

				case d := <-sess.Deliveries:

					if err := broker.Ack(d.AckToken); err != nil {
						t.Errorf("ack failed: %v", err)
						return
					}

					acked.Add(1)

				case <-ctx.Done():
					return
				}
			}

		}(s)
	}

	var producerWG sync.WaitGroup

	for p := 0; p < producers; p++ {

		producerWG.Add(1)

		go func(id int) {
			defer producerWG.Done()

			for i := 0; i < perProducer; i++ {

				err := broker.Publish(model.Message{
					Payload: fmt.Sprintf(
						"producer-%d-%d",
						id,
						i,
					),
				})

				if err != nil {
					t.Errorf("publish failed: %v", err)
				}
			}

		}(p)
	}

	producerWG.Wait()

	deadline := time.After(10 * time.Second)

	for {
		if acked.Load() == totalMessages {
			break
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for acknowledgements")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()

	consumerWG.Wait()

	waitForBrokerIdle(t, broker)

	metrics := broker.Metrics()

	if metrics.ProducedCount != totalMessages {
		t.Fatalf(
			"expected %d produced got %d",
			totalMessages,
			metrics.ProducedCount,
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
		t.Fatalf(
			"queue not empty (%d)",
			metrics.QueueDepth,
		)
	}

	if metrics.InflightCount != 0 {
		t.Fatalf(
			"inflight not empty (%d)",
			metrics.InflightCount,
		)
	}
}
