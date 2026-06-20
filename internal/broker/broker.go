package broker

import "sync"

func NewBroker(capacity int) *Queue {
	q := &Queue{
		readyQueue: make([]Message, capacity),
		cap:        capacity,
		size:       0,
		head:       0,
		tail:       0,
		closed:     false,
		inFlight:   make(map[string]InFlightMessage),
	}

	q.condProd = sync.NewCond(&q.mu)
	q.condCons = sync.NewCond(&q.mu)
	return q
}
