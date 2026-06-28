package protocol

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type Request struct {
	Type     string        `json:"type"`
	Message  model.Message `json:"message,omitzero"`
	AckToken string        `json:"ack_token,omitempty"`
}
