package model

type Metric struct {
	ProducedCount        int
	AckedCount           int
	DlqCount             int
	InflightCount        int
	RedeliveredCount     int
	ConsumerSessionCount int
	QueueDepth           int
}

type QueueMetric struct {
	Name string `json:"name"`
	Metric
}

type BrokerMetrics struct {
	Queues []QueueMetric `json:"queues"`
	Total  Metric        `json:"total"`
}
