package queue

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type DLQ struct {
	messages []model.Message
}

func NewDLQ() *DLQ {
	return &DLQ{
		messages: make([]model.Message, 0),
	}
}

func (q *DLQ) Push(message model.Message) {
	q.messages = append(q.messages, message)
}

func (q *DLQ) Size() int {
	return len(q.messages)
}
