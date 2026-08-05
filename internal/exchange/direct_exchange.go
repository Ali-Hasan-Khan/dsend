package exchange

type DirectExchange struct {
	*baseExchange
}

func NewDirectExchange(name string) *DirectExchange {
	return &DirectExchange{
		baseExchange: NewBaseExchange(name),
	}
}

func (e *DirectExchange) Route(routingKey string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	queues := make([]string, 0)
	for _, b := range e.bindings {
		if b.BindingKey == routingKey {
			queues = append(queues, b.QueueName)
		}
	}

	return queues
}
