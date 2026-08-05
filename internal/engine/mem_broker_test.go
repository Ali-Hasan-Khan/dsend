package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

func newMultiQueueBroker(t *testing.T) *InMemoryBroker {
	t.Helper()

	cfg := DefaultConfig()
	cfg.QueueSize = 10

	broker := NewInMemoryBroker(
		cfg,
		&mockWAL{},
		make(map[string]*QueueRuntime),
		make(map[string]*QueueRuntime),
	)
	broker.queues[model.DefaultQueueName] = broker.newQueueRuntime(
		model.DefaultQueueName,
		nil,
	)

	broker.Start(context.Background())
	t.Cleanup(broker.Shutdown)

	return broker
}

func receiveDelivery(t *testing.T, consumer *session.ConsumerSession) model.Delivery {
	t.Helper()

	select {
	case delivery := <-consumer.Deliveries:
		return delivery
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivery")
		return model.Delivery{}
	}
}

func TestInMemoryBrokerQueueLifecycle(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	if err := broker.CreateQueue("orders"); !errors.Is(err, ErrQueueExists) {
		t.Fatalf("expected ErrQueueExists, got %v", err)
	}

	if err := broker.CreateQueue(""); !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("expected ErrInvalidQueue, got %v", err)
	}

	wantQueues := []string{model.DefaultQueueName, "orders"}
	if got := broker.ListQueues(); !reflect.DeepEqual(got, wantQueues) {
		t.Fatalf("queues = %v, want %v", got, wantQueues)
	}

	if err := broker.DeleteQueue(model.DefaultQueueName); !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("expected ErrInvalidQueue, got %v", err)
	}

	if err := broker.DeleteQueue("orders"); err != nil {
		t.Fatalf("delete queue: %v", err)
	}

	if _, err := broker.QueueMetrics("orders"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got %v", err)
	}
}

func TestInMemoryBrokerRoutesMessagesToTheirQueue(t *testing.T) {
	broker := newMultiQueueBroker(t)

	for _, name := range []string{"orders", "payments"} {
		if err := broker.CreateQueue(name); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if err := broker.BindQueue(model.DefaultExchangeName, name, name); err != nil {
			t.Fatalf("bind %s: %v", name, err)
		}
	}

	ordersConsumer := session.NewConsumerSession("orders-consumer")
	paymentsConsumer := session.NewConsumerSession("payments-consumer")

	if err := broker.Subscribe("orders", ordersConsumer); err != nil {
		t.Fatalf("subscribe orders: %v", err)
	}
	if err := broker.Subscribe("payments", paymentsConsumer); err != nil {
		t.Fatalf("subscribe payments: %v", err)
	}

	if err := broker.Publish(model.DefaultExchangeName, "orders", model.Message{Payload: "order-1"}); err != nil {
		t.Fatalf("publish orders: %v", err)
	}
	if err := broker.Publish(model.DefaultExchangeName, "payments", model.Message{Payload: "payment-1"}); err != nil {
		t.Fatalf("publish payments: %v", err)
	}

	order := receiveDelivery(t, ordersConsumer)
	if order.Payload != "order-1" {
		t.Fatalf("orders consumer received %q", order.Payload)
	}

	payment := receiveDelivery(t, paymentsConsumer)
	if payment.Payload != "payment-1" {
		t.Fatalf("payments consumer received %q", payment.Payload)
	}

	if err := broker.Ack(order.AckToken); err != nil {
		t.Fatalf("ack order: %v", err)
	}
	if err := broker.Ack(payment.AckToken); err != nil {
		t.Fatalf("ack payment: %v", err)
	}

	for _, name := range []string{"orders", "payments"} {
		metric, err := broker.QueueMetrics(name)
		if err != nil {
			t.Fatalf("metrics for %s: %v", name, err)
		}
		if metric.AckedCount != 1 || metric.QueueDepth != 0 || metric.InflightCount != 0 {
			t.Fatalf("unexpected %s metrics: %+v", name, metric)
		}
	}
}

func TestInMemoryBrokerRejectsDeletingNonEmptyQueue(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := broker.BindQueue(model.DefaultExchangeName, "orders", "orders"); err != nil {
		t.Fatalf("bind queue: %v", err)
	}
	if err := broker.Publish(model.DefaultExchangeName, "orders", model.Message{Payload: "order-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if err := broker.DeleteQueue("orders"); !errors.Is(err, ErrQueueNotEmpty) {
		t.Fatalf("expected ErrQueueNotEmpty, got %v", err)
	}

	if _, err := broker.QueueMetrics("orders"); err != nil {
		t.Fatalf("queue should remain available: %v", err)
	}
}
