package protocol

const (
	PublishRequest     = "publish"
	ConsumeRequest     = "consume"
	AckRequest         = "ack"
	MetricsRequest     = "metrics"
	SubscribeRequest   = "subscribe"
	UnsubscribeRequest = "unsubscribe"
)

const (
	CreateQueueRequest = "create_queue"
	DeleteQueueRequest = "delete_queue"
	ListQueuesRequest  = "list_queues"
)
