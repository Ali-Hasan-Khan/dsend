package exchange

type FanOutExchange struct {
	*baseExchange
}

func NewFanOutExchange(name string) *FanOutExchange {
	return &FanOutExchange{
		baseExchange: NewBaseExchange(name),
	}
}

func (e *FanOutExchange) Route(routingKey string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	queues := make([]string, 0)
	for _, b := range e.bindings {
		queues = append(queues, b.QueueName)
	}

	return queues
}
