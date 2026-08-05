package exchange

import (
	"reflect"
	"testing"
)

func TestFanOutExchangeRoute(t *testing.T) {
	e := NewFanOutExchange("fanout")

	if got := e.Route("any"); len(got) != 0 {
		t.Fatalf("expected no routes, got %v", got)
	}

	e.Bind("ignored-a", "q1")
	e.Bind("ignored-b", "q2")
	e.Bind("ignored-a", "q3")

	got := sortedRoutes(e.Route("whatever"))
	want := []string{"q1", "q2", "q3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("route = %v, want %v", got, want)
	}
}
