package exchange

import (
	"reflect"
	"testing"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

func TestNewExchangeFactory(t *testing.T) {
	tests := []struct {
		name         string
		exchangeType model.ExchangeType
		wantType     reflect.Type
	}{
		{"direct", model.DirectExchange, reflect.TypeOf(&DirectExchange{})},
		{"fanout", model.FanOutExchange, reflect.TypeOf(&FanOutExchange{})},
		{"topic", model.TopicExchange, reflect.TypeOf(&TopicExchange{})},
		{"unknown defaults to direct", model.ExchangeType("unknown"), reflect.TypeOf(&DirectExchange{})},
	}

	for _, tt := range tests {
		got := NewExchangeFactory("ex", tt.exchangeType)
		if typ := reflect.TypeOf(got); typ != tt.wantType {
			t.Fatalf("NewExchangeFactory(%s) = %v, want %v", tt.name, typ, tt.wantType)
		}
	}
}
