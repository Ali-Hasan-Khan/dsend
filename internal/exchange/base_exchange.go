package exchange

import (
	"errors"
	"slices"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

var (
	ErrBindingNotExist     = errors.New("binding does not exist")
	ErrBindingAlreadyExist = errors.New("binding already exists")
)

type baseExchange struct {
	Name     string
	bindings []model.Binding

	mu sync.Mutex
}

func NewBaseExchange(name string) *baseExchange {
	return &baseExchange{
		Name:     name,
		bindings: make([]model.Binding, 0),
	}
}

func (e *baseExchange) checkBindingExists(bindingKey, queueName string) bool {
	for _, binding := range e.bindings {
		if binding.BindingKey == bindingKey && binding.QueueName == queueName {
			return true
		}
	}
	return false
}

func (e *baseExchange) hasBinding(bindingKey, queueName string) bool {
	for _, binding := range e.bindings {
		if binding.QueueName != queueName {
			continue
		}
		if bindingKey == "" || binding.BindingKey == bindingKey {
			return true
		}
	}
	return false
}

func (e *baseExchange) Bind(bindingKey string, queueName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if exists := e.checkBindingExists(bindingKey, queueName); exists {
		return ErrBindingAlreadyExist
	}

	e.bindings = append(e.bindings, model.Binding{
		QueueName:  queueName,
		BindingKey: bindingKey,
	})

	return nil
}

func (e *baseExchange) Unbind(bindingKey, queueName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if exists := e.hasBinding(bindingKey, queueName); !exists {
		return ErrBindingNotExist
	}

	e.bindings = slices.DeleteFunc(e.bindings, func(b model.Binding) bool {
		if bindingKey != "" {
			return b.BindingKey == bindingKey && b.QueueName == queueName
		}
		return b.QueueName == queueName
	})

	return nil
}

func (e *baseExchange) ListBindings() []model.Binding {
	e.mu.Lock()
	defer e.mu.Unlock()
	bindings := make([]model.Binding, 0, len(e.bindings))
	for _, b := range e.bindings {
		bindings = append(bindings, b)
	}
	return bindings
}
