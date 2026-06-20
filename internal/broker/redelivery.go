package broker

type ExpiredDelivery struct {
	token   string
	message Message
}
