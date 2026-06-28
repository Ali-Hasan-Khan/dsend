package protocol

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type Response struct {
	Success  bool          `json:"success"`
	Error    string        `json:"error,omitempty"`
	Message  model.Message `json:"message,omitzero"`
	AckToken string        `json:"ack_token,omitempty"`
	Metrics  model.Metric  `json:"metrics,omitzero"`
}
