package exchange

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type Exchange interface {
	Route(routingKey string) []string
	Bind(bindingKey, queueName string) error
	Unbind(bindingKey, queueName string) error
	ListBindings() []model.Binding
}

func NewExchangeFactory(exchangeName string, exchangeType model.ExchangeType) Exchange {
	switch exchangeType {
	case model.FanOutExchange:
		return NewFanOutExchange(exchangeName)
	case model.TopicExchange:
		return NewTopicExchange(exchangeName)
	default:
		return NewDirectExchange(exchangeName)
	}
}
