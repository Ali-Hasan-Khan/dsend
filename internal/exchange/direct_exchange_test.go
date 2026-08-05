package exchange

import (
	"reflect"
	"testing"
)

func TestDirectExchangeRoute(t *testing.T) {
	e := NewDirectExchange("direct")

	if got := e.Route("orders"); len(got) != 0 {
		t.Fatalf("expected no routes, got %v", got)
	}

	e.Bind("orders", "orders-queue")
	e.Bind("orders", "orders-backup")
	e.Bind("payments", "payments-queue")

	if got, want := sortedRoutes(e.Route("orders")), []string{"orders-backup", "orders-queue"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("route(orders) = %v, want %v", got, want)
	}

	if got, want := sortedRoutes(e.Route("payments")), []string{"payments-queue"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("route(payments) = %v, want %v", got, want)
	}

	if got := e.Route("shipments"); len(got) != 0 {
		t.Fatalf("expected no route for shipments, got %v", got)
	}
}
