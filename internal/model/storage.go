package model

type RecordType string

const (
	QueueCreated RecordType = "queue_created"
	QueueDeleted RecordType = "queue_deleted"
	Published    RecordType = "published"
	Acknowledged RecordType = "acknowledged"
	Requeued     RecordType = "requeued"
	DeadLettered RecordType = "dead_lettered"
)

type Record struct {
	Type      RecordType `json:"type"`
	Queue     string     `json:"queue"`
	Message   Message    `json:"message,omitempty"`
	MessageID string     `json:"message_id,omitempty"`
}
