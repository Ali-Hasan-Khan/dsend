package model

import "time"

type Message struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
	Retry     int       `json:"retry"`
}

type InFlightMessage struct {
	Message
	DeliveredAt time.Time `json:"delivered_at"`
}
