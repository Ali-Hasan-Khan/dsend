package model

type Delivery struct {
	Message
	AckToken   string
	ConsumerID string
}
