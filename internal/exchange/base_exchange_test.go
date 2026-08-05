package exchange

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

func sortedRoutes(routes []string) []string {
	out := append([]string(nil), routes...)
	slices.Sort(out)
	return out
}

func TestBaseExchangeBind(t *testing.T) {
	e := NewBaseExchange("ex")

	if err := e.Bind("orders.created", "orders"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	if err := e.Bind("orders.created", "orders"); !errors.Is(err, ErrBindingAlreadyExist) {
		t.Fatalf("expected ErrBindingAlreadyExist, got %v", err)
	}

	if err := e.Bind("orders.cancelled", "orders"); err != nil {
		t.Fatalf("bind same queue with different key: %v", err)
	}

	want := []model.Binding{
		{QueueName: "orders", BindingKey: "orders.created"},
		{QueueName: "orders", BindingKey: "orders.cancelled"},
	}
	if got := e.ListBindings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("bindings = %v, want %v", got, want)
	}
}

func TestBaseExchangeUnbind(t *testing.T) {
	e := NewBaseExchange("ex")

	for _, b := range []model.Binding{
		{QueueName: "orders", BindingKey: "orders.created"},
		{QueueName: "payments", BindingKey: "payments.created"},
		{QueueName: "orders", BindingKey: "orders.cancelled"},
	} {
		if err := e.Bind(b.BindingKey, b.QueueName); err != nil {
			t.Fatalf("bind %v: %v", b, err)
		}
	}

	if err := e.Unbind("payments.created", "orders"); !errors.Is(err, ErrBindingNotExist) {
		t.Fatalf("expected ErrBindingNotExist, got %v", err)
	}

	if err := e.Unbind("orders.created", "orders"); err != nil {
		t.Fatalf("unbind: %v", err)
	}

	want := []model.Binding{
		{QueueName: "payments", BindingKey: "payments.created"},
		{QueueName: "orders", BindingKey: "orders.cancelled"},
	}
	if got := e.ListBindings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("bindings = %v, want %v", got, want)
	}
}

func TestBaseExchangeUnbindAllForQueue(t *testing.T) {
	e := NewBaseExchange("ex")

	for _, b := range []model.Binding{
		{QueueName: "orders", BindingKey: "a"},
		{QueueName: "orders", BindingKey: "b"},
		{QueueName: "payments", BindingKey: "a"},
	} {
		if err := e.Bind(b.BindingKey, b.QueueName); err != nil {
			t.Fatalf("bind %v: %v", b, err)
		}
	}

	if err := e.Unbind("", "orders"); err != nil {
		t.Fatalf("unbind all for queue: %v", err)
	}

	want := []model.Binding{{QueueName: "payments", BindingKey: "a"}}
	if got := e.ListBindings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("bindings = %v, want %v", got, want)
	}
}

func TestBaseExchangeListBindingsReturnsCopy(t *testing.T) {
	e := NewBaseExchange("ex")

	if err := e.Bind("a", "orders"); err != nil {
		t.Fatal(err)
	}

	bindings := e.ListBindings()
	bindings[0].QueueName = "mutated"

	want := []model.Binding{{QueueName: "orders", BindingKey: "a"}}
	if got := e.ListBindings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("bindings = %v, want %v", got, want)
	}
}
