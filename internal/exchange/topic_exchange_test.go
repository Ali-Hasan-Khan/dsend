package exchange

import (
	"reflect"
	"testing"
)

func TestTopicExchangeMatch(t *testing.T) {
	tests := []struct {
		bindingKey string
		routingKey string
		want       bool
	}{
		// exact matches
		{"orders.created", "orders.created", true},
		{"orders", "orders", true},

		// exact mismatches
		{"orders.created", "orders.cancelled", false},
		{"orders.created", "orders", false},
		{"orders", "orders.created", false},
		{"orders.created", "orders.created.refunded", false},

		// single-word wildcard
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.cancelled", true},
		{"orders.*", "orders", false},
		{"orders.*", "orders.created.refunded", false},
		{"orders.*", "payments.created", false},
		{"*.created", "orders.created", true},
		{"*.created", "orders.cancelled", false},
		{"a.*.b", "a.x.b", true},
		{"a.*.b", "a.x.y", false},
		{"a.*.b", "a.b", false},

		// multi-word wildcard
		{"orders.#", "orders.created", true},
		{"orders.#", "orders.created.refunded", true},
		{"orders.#", "orders", true},
		{"orders.#", "payments.created", false},
		{"#", "orders.created", true},
		{"#", "orders", true},

		// trailing # where binding has more parts than routing
		{"a.#", "a", true},
		{"a.b.#", "a.b", true},
		{"a.b.#", "a.b.c", true},
		{"a.b.#", "a.c", false},

		// # is only valid at the end
		{"a.#.b", "a.x.b", false},

		// binding longer or shorter than routing without wildcard
		{"a.b", "a.b.c", false},
		{"a.b.c", "a.b", false},
	}

	for _, tt := range tests {
		e := NewTopicExchange("topics")
		if got := e.match(tt.bindingKey, tt.routingKey); got != tt.want {
			t.Errorf("match(%q, %q) = %v, want %v", tt.bindingKey, tt.routingKey, got, tt.want)
		}
	}
}

func TestTopicExchangeRoute(t *testing.T) {
	e := NewTopicExchange("topics")

	e.Bind("orders.*", "orders-queue")
	e.Bind("orders.#", "orders-all")
	e.Bind("payments.#", "payments-queue")

	if got, want := sortedRoutes(e.Route("orders.created")), []string{"orders-all", "orders-queue"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("route(orders.created) = %v, want %v", got, want)
	}

	if got, want := sortedRoutes(e.Route("orders")), []string{"orders-all"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("route(orders) = %v, want %v", got, want)
	}

	if got, want := sortedRoutes(e.Route("payments.received")), []string{"payments-queue"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("route(payments.received) = %v, want %v", got, want)
	}

	if got := e.Route("shipments.created"); len(got) != 0 {
		t.Fatalf("expected no route, got %v", got)
	}
}
