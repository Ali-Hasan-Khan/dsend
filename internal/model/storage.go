package model

type RecordType string

const (
	ExchangeCreated RecordType = "exchange_created"
	ExchangeDeleted RecordType = "exchange_deleted"
	QueueCreated    RecordType = "queue_created"
	QueueDeleted    RecordType = "queue_deleted"
	QueueBinded     RecordType = "queue_binded"
	QueueUnbinded   RecordType = "queue_unbinded"
	Published       RecordType = "published"
	Acknowledged    RecordType = "acknowledged"
	Requeued        RecordType = "requeued"
	DeadLettered    RecordType = "dead_lettered"
)

type Record struct {
	Type         RecordType `json:"type,omitempty"`
	Exchange     string     `json:"exchange,omitempty"`
	ExchangeType string     `json:"exchange_type,omitempty"`
	Queue        string     `json:"queue,omitempty"`
	Message      Message    `json:"message,omitzero"`
	MessageID    string     `json:"message_id,omitempty"`
	BindingKey   string     `json:"binding_key,omitempty"`
}
