package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

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

	broker := NewInMemoryBroker(
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
