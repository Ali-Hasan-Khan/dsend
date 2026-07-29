package inflight

import (
	"sync"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type Manager struct {
	mu         sync.RWMutex
	deliveries map[string]model.InFlightMessage
}

func NewManager() *Manager {
	return &Manager{
		deliveries: make(map[string]model.InFlightMessage),
	}
}

func (m *Manager) Add(token string, message model.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveries[token] = model.InFlightMessage{
		Message:     message,
		DeliveredAt: time.Now(),
	}
}

func (m *Manager) Remove(token string) (model.Message, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.deliveries[token]; !ok {
		return model.Message{}, false
	}
	inflight := m.deliveries[token]
	delete(m.deliveries, token)
	return inflight.Message, true
}

func (m *Manager) Get(token string) (model.Message, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	delivery, ok := m.deliveries[token]
	if !ok {
		return model.Message{}, false
	}

	return delivery.Message, true
}

func (m *Manager) Expired(timeout time.Duration) []model.Delivery {
	m.mu.RLock()
	defer m.mu.RUnlock()
	expiredMessages := make([]model.Delivery, 0)
	for idx, item := range m.deliveries {
		if time.Since(item.DeliveredAt) > timeout {
			expiredMessages = append(expiredMessages, model.Delivery{
				Message:  item.Message,
				AckToken: idx,
			})
		}
	}
	return expiredMessages
}

func (m *Manager) IsPresent(token string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.deliveries[token]; !ok {
		return false
	}
	return true
}

func (m *Manager) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.deliveries)
}
