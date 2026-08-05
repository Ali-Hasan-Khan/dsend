package exchange

import "strings"

type TopicExchange struct {
	*baseExchange
}

func NewTopicExchange(name string) *TopicExchange {
	return &TopicExchange{
		baseExchange: NewBaseExchange(name),
	}
}

func (e *TopicExchange) match(bindingKey, routingKey string) bool {
	bindingParts := strings.Split(bindingKey, ".")
	routingParts := strings.Split(routingKey, ".")

	bi := 0
	ri := 0
	for bi < len(bindingParts) && ri < len(routingParts) {
		btoken := bindingParts[bi]
		rtoken := routingParts[ri]
		switch btoken {
		case rtoken:
			bi++
			ri++
		case "*":
			bi++
			ri++
		case "#":
			return bi == len(bindingParts)-1
		default:
			return false
		}
	}

	if bi == len(bindingParts)-1 &&
		bindingParts[bi] == "#" {
		return true
	}

	return bi == len(bindingParts) && ri == len(routingParts)
}

func (e *TopicExchange) Route(routingKey string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	queues := make([]string, 0)
	for _, b := range e.bindings {
		if e.match(b.BindingKey, routingKey) {
			queues = append(queues, b.QueueName)
		}
	}

	return queues
}
