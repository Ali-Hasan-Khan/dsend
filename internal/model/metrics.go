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
