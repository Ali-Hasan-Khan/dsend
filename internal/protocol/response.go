package protocol

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type Response struct {
	Success  bool                `json:"success"`
	Error    string              `json:"error,omitempty"`
	Queue    string              `json:"queue,omitempty"`
	Message  model.Message       `json:"message,omitzero"`
	AckToken string              `json:"ack_token,omitempty"`
	Metrics  model.BrokerMetrics `json:"broker_metrics,omitzero"`
	Queues   []model.QueueMetric `json:"queues,omitzero"`
}
