package protocol

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type Request struct {
	ID           string        `json:"id,omitempty"`
	Type         string        `json:"type"`
	Queue        string        `json:"queue,omitempty"`
	Payload      model.Message `json:"message,omitzero"`
	AckToken     string        `json:"ack_token,omitempty"`
	Exchange     string        `json:"exchange,omitempty"`
	ExchangeType string        `json:"exchange_type,omitempty"`
	RoutingKey   string        `json:"routing_key,omitempty"`
	BindingKey   string        `json:"binding_key,omitempty"`
}
