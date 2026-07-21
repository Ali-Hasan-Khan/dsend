package client

type ReceivedMessage struct {
	ID       string
	Payload  string
	AckToken string
}
