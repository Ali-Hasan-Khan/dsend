package broker

import "time"

type Message struct {
	ID        string
	Payload   []byte
	Timestamp time.Time
	Retry     int
}

type InFlightMessage struct {
	Message
	DeliveredAt time.Time
}
