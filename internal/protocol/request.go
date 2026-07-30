package protocol

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type Request struct {
	ID       string        `json:"id,omitempty"`
	Type     string        `json:"type"`
	Queue    string        `json:"queue,omitempty"`
	Message  model.Message `json:"message,omitzero"`
	AckToken string        `json:"ack_token,omitempty"`
}
