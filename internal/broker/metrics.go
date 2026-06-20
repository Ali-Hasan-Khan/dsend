package broker

type Metric struct {
	ProducedCount int
	AckedCount    int
	DlqCount      int
	InflightCount int
}
