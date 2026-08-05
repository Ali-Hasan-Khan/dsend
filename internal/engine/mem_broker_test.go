package engine

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/exchange"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
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

func assertNoDelivery(t *testing.T, consumer *session.ConsumerSession) {
	t.Helper()

	select {
	case d := <-consumer.Deliveries:
		t.Fatalf("unexpected delivery: %+v", d)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestInMemoryBrokerExchangeLifecycle(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if want := []string{model.DefaultExchangeName}; !reflect.DeepEqual(broker.ListExchanges(), want) {
		t.Fatalf("exchanges = %v, want %v", broker.ListExchanges(), want)
	}

	if err := broker.CreateExchange("events", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateExchange("events", "direct"); !errors.Is(err, ErrExchangeExists) {
		t.Fatalf("expected ErrExchangeExists, got %v", err)
	}
	if err := broker.CreateExchange(model.DefaultExchangeName, "direct"); !errors.Is(err, ErrDefaultExchange) {
		t.Fatalf("expected ErrDefaultExchange, got %v", err)
	}
	if err := broker.CreateExchange("", "direct"); !errors.Is(err, ErrInvalidExchange) {
		t.Fatalf("expected ErrInvalidExchange, got %v", err)
	}
	if err := broker.CreateExchange("events", "invalid-type"); !errors.Is(err, ErrInvalidExchangeType) {
		t.Fatalf("expected ErrInvalidExchangeType, got %v", err)
	}

	want := []string{model.DefaultExchangeName, "events"}
	if got := broker.ListExchanges(); !reflect.DeepEqual(got, want) {
		t.Fatalf("exchanges = %v, want %v", got, want)
	}

	if err := broker.DeleteExchange("events"); err != nil {
		t.Fatalf("delete exchange: %v", err)
	}
	if err := broker.DeleteExchange("events"); !errors.Is(err, ErrExchangeNotFound) {
		t.Fatalf("expected ErrExchangeNotFound, got %v", err)
	}
	if err := broker.DeleteExchange(model.DefaultExchangeName); !errors.Is(err, ErrDefaultExchange) {
		t.Fatalf("expected ErrDefaultExchange, got %v", err)
	}
}

func TestInMemoryBrokerDeleteExchangeWithBindings(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateExchange("events", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := broker.BindQueue("events", "orders", "orders"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	if err := broker.DeleteExchange("events"); !errors.Is(err, ErrExchangeNotEmpty) {
		t.Fatalf("expected ErrExchangeNotEmpty, got %v", err)
	}

	if err := broker.UnbindQueue("events", "orders", "orders"); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if err := broker.DeleteExchange("events"); err != nil {
		t.Fatalf("delete exchange: %v", err)
	}
}

func TestInMemoryBrokerBindQueueErrors(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateExchange("events", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	if err := broker.BindQueue("missing", "orders", "k"); !errors.Is(err, ErrExchangeNotFound) {
		t.Fatalf("expected ErrExchangeNotFound, got %v", err)
	}
	if err := broker.BindQueue("events", "missing", "k"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got %v", err)
	}
	if err := broker.BindQueue("events", "orders", "k"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := broker.BindQueue("events", "orders", "k"); !errors.Is(err, exchange.ErrBindingAlreadyExist) {
		t.Fatalf("expected ErrBindingAlreadyExist, got %v", err)
	}
}

func TestInMemoryBrokerUnbindQueueStopsRouting(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateExchange("events", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := broker.BindQueue("events", "orders", "orders"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	consumer := session.NewConsumerSession("c")
	if err := broker.Subscribe("orders", consumer); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := broker.Publish("events", "orders", model.Message{Payload: "m1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if d := receiveDelivery(t, consumer); d.Payload != "m1" {
		t.Fatalf("received %q", d.Payload)
	}

	if err := broker.UnbindQueue("events", "orders", "orders"); err != nil {
		t.Fatalf("unbind: %v", err)
	}

	if err := broker.Publish("events", "orders", model.Message{Payload: "m2"}); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("expected ErrNoRoute after unbind, got %v", err)
	}

	if err := broker.UnbindQueue("events", "orders", "orders"); !errors.Is(err, exchange.ErrBindingNotExist) {
		t.Fatalf("expected ErrBindingNotExist, got %v", err)
	}
	if err := broker.UnbindQueue("missing", "orders", "k"); !errors.Is(err, ErrExchangeNotFound) {
		t.Fatalf("expected ErrExchangeNotFound, got %v", err)
	}
}

func TestInMemoryBrokerDirectRouting(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateExchange("orders-ex", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := broker.BindQueue("orders-ex", "orders", "orders"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	consumer := session.NewConsumerSession("c")
	if err := broker.Subscribe("orders", consumer); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := broker.Publish("orders-ex", "orders", model.Message{Payload: "order-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	d := receiveDelivery(t, consumer)
	if d.Payload != "order-1" {
		t.Fatalf("received %q", d.Payload)
	}

	if err := broker.Ack(d.AckToken); err != nil {
		t.Fatalf("ack: %v", err)
	}

	metric, err := broker.QueueMetrics("orders")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if metric.AckedCount != 1 || metric.QueueDepth != 0 || metric.InflightCount != 0 {
		t.Fatalf("unexpected metrics: %+v", metric)
	}

	if err := broker.Publish("orders-ex", "payments", model.Message{Payload: "x"}); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("expected ErrNoRoute, got %v", err)
	}
	if err := broker.Publish("missing-ex", "orders", model.Message{Payload: "x"}); !errors.Is(err, ErrExchangeNotFound) {
		t.Fatalf("expected ErrExchangeNotFound, got %v", err)
	}
}

func TestInMemoryBrokerFanoutPublishesToAllBoundQueues(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateExchange("broadcast", "fanout"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}

	consumers := make(map[string]*session.ConsumerSession)
	for _, name := range []string{"q1", "q2", "q3"} {
		if err := broker.CreateQueue(name); err != nil {
			t.Fatalf("create queue %s: %v", name, err)
		}
		if err := broker.BindQueue("broadcast", name, "ignored"); err != nil {
			t.Fatalf("bind %s: %v", name, err)
		}
		consumers[name] = session.NewConsumerSession(name + "-consumer")
		if err := broker.Subscribe(name, consumers[name]); err != nil {
			t.Fatalf("subscribe %s: %v", name, err)
		}
	}

	if err := broker.Publish("broadcast", "any-key", model.Message{Payload: "broadcast-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	for _, name := range []string{"q1", "q2", "q3"} {
		if d := receiveDelivery(t, consumers[name]); d.Payload != "broadcast-1" {
			t.Fatalf("%s received %q", name, d.Payload)
		}
	}
}

func TestInMemoryBrokerTopicRouting(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateExchange("topics", "topic"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}

	type binding struct {
		queue string
		key   string
	}
	bindings := []binding{
		{"orders-queue", "orders.*"},
		{"orders-all", "orders.#"},
		{"payments-queue", "payments.#"},
	}

	consumers := make(map[string]*session.ConsumerSession)
	for _, b := range bindings {
		if err := broker.CreateQueue(b.queue); err != nil {
			t.Fatalf("create queue %s: %v", b.queue, err)
		}
		if err := broker.BindQueue("topics", b.queue, b.key); err != nil {
			t.Fatalf("bind %s: %v", b.queue, err)
		}
		consumers[b.queue] = session.NewConsumerSession(b.queue)
		if err := broker.Subscribe(b.queue, consumers[b.queue]); err != nil {
			t.Fatalf("subscribe %s: %v", b.queue, err)
		}
	}

	if err := broker.Publish("topics", "orders.created", model.Message{Payload: "order-event"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for _, q := range []string{"orders-queue", "orders-all"} {
		if d := receiveDelivery(t, consumers[q]); d.Payload != "order-event" {
			t.Fatalf("%s received %q", q, d.Payload)
		}
	}
	assertNoDelivery(t, consumers["payments-queue"])

	if err := broker.Publish("topics", "orders", model.Message{Payload: "bare-order"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if d := receiveDelivery(t, consumers["orders-all"]); d.Payload != "bare-order" {
		t.Fatalf("orders-all received %q", d.Payload)
	}
	assertNoDelivery(t, consumers["orders-queue"])

	if err := broker.Publish("topics", "payments.received", model.Message{Payload: "payment-event"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if d := receiveDelivery(t, consumers["payments-queue"]); d.Payload != "payment-event" {
		t.Fatalf("payments-queue received %q", d.Payload)
	}
	assertNoDelivery(t, consumers["orders-queue"])
}

func TestInMemoryBrokerRejectsInvalidBindingKeys(t *testing.T) {
	broker := newMultiQueueBroker(t)

	if err := broker.CreateExchange("events", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	invalid := []string{
		"",
		"sads...dsds",
		"sds.#.sdg",
		"a..b",
		".orders",
		"orders.",
		"orders..#",
		"orders.#.extra",
		"a b",
		"a#b",
		"a*b",
		"a-b*c",
	}

	for _, key := range invalid {
		if err := broker.BindQueue("events", "orders", key); !errors.Is(err, ErrInvalidBindingKey) {
			t.Fatalf("BindQueue(%q) = %v, want ErrInvalidBindingKey", key, err)
		}
	}

	valid := []string{
		"orders",
		"orders.created",
		"orders.*",
		"*.created",
		"a.*.b",
		"orders.#",
		"#",
		"a.b.c",
		"orders-queue",
		"order_123",
	}

	for _, key := range valid {
		if err := broker.BindQueue("events", "orders", key); err != nil {
			t.Fatalf("BindQueue(%q) = %v, want nil", key, err)
		}
		if err := broker.UnbindQueue("events", "orders", key); err != nil {
			t.Fatalf("UnbindQueue(%q) = %v, want nil", key, err)
		}
	}
}

func TestInMemoryBrokerRecoversExchangesAndBindings(t *testing.T) {
	wal, err := storage.NewFileWAL(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	broker, err := NewBroker(DefaultConfig(), wal)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}

	if err := broker.CreateExchange("events", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := broker.BindQueue("events", "orders", "orders"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := broker.Publish("events", "orders", model.Message{Payload: "order-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	broker.Shutdown()

	broker2, err := NewBroker(DefaultConfig(), wal)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer broker2.Shutdown()

	if !slices.Contains(broker2.ListExchanges(), "events") {
		t.Fatalf("exchanges after restart = %v, want to contain events", broker2.ListExchanges())
	}

	if err := broker2.Publish("events", "orders", model.Message{Payload: "order-2"}); err != nil {
		t.Fatalf("publish after restart: %v", err)
	}

	metric, err := broker2.QueueMetrics("orders")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if metric.QueueDepth != 2 {
		t.Fatalf("queue depth = %d, want 2 (recovered message + new publish)", metric.QueueDepth)
	}
}

func TestInMemoryBrokerRestartsWithDefaultExchangeBinding(t *testing.T) {
	wal, err := storage.NewFileWAL(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	broker, err := NewBroker(DefaultConfig(), wal)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	broker.Shutdown()

	broker2, err := NewBroker(DefaultConfig(), wal)
	if err != nil {
		t.Fatalf("second start must not panic: %v", err)
	}
	defer broker2.Shutdown()

	if err := broker2.Publish(model.DefaultExchangeName, model.DefaultQueueName, model.Message{Payload: "hello"}); err != nil {
		t.Fatalf("publish to default exchange: %v", err)
	}
}

func TestInMemoryBrokerRestartsAfterDeletingBoundQueue(t *testing.T) {
	wal, err := storage.NewFileWAL(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	broker, err := NewBroker(DefaultConfig(), wal)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}

	if err := broker.CreateExchange("events", "direct"); err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if err := broker.CreateQueue("orders"); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := broker.BindQueue("events", "orders", "orders"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := broker.DeleteQueue("orders"); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
	broker.Shutdown()

	broker2, err := NewBroker(DefaultConfig(), wal)
	if err != nil {
		t.Fatalf("restart after deleting bound queue: %v", err)
	}
	defer broker2.Shutdown()

	if err := broker2.Publish("events", "orders", model.Message{Payload: "x"}); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("expected ErrNoRoute after queue deletion, got %v", err)
	}
}
