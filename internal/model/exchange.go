package model

type Binding struct {
	QueueName  string
	BindingKey string
}

type ExchangeType string

const (
	DirectExchange ExchangeType = "direct"
	FanOutExchange ExchangeType = "fanout"
	TopicExchange  ExchangeType = "topic"
)
