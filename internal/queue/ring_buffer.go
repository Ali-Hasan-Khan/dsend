package queue

import (
	"fmt"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type RingBufferQueue struct {
	items []model.Message
	head  int
	tail  int
	size  int
	cap   int
}

func NewRingBufferQueue(capacity int) *RingBufferQueue {
	cq := &RingBufferQueue{
		items: make([]model.Message, capacity),
		cap:   capacity,
	}
	return cq
}

func (q *RingBufferQueue) Push(message model.Message) {
	q.items[q.tail] = message
	q.tail = (q.tail + 1) % q.cap
	q.size++
}

func (q *RingBufferQueue) Pop() model.Message {
	item := q.items[q.head]
	q.items[q.head] = model.Message{}
	q.head = (q.head + 1) % q.cap
	q.size--
	return item
}

func (q *RingBufferQueue) Peek() model.Message {
	item := q.items[q.head]
	return item
}

func (q *RingBufferQueue) Size() int {
	return q.size
}

func (q *RingBufferQueue) Capacity() int {
	return q.cap
}

func (q *RingBufferQueue) String() string {
	return fmt.Sprint(q.items)
}
