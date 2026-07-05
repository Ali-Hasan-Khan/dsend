package session

import (
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type ConsumerSession struct {
	ID string

	Deliveries chan model.Delivery

	Closed chan struct{}
}

func NewConsumerSession(ID string) *ConsumerSession {
	return &ConsumerSession{
		ID:         ID,
		Deliveries: make(chan model.Delivery, 100),
		Closed:     make(chan struct{}),
	}
}

func (cs *ConsumerSession) Close() {
	close(cs.Closed)
}
