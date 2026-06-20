package broker

type Delivery struct {
	Message
	AckToken string
}
