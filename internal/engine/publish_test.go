package engine

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
)

type mockWAL struct {
	mu        sync.Mutex
	appendErr error
	messages  []model.Message
}

func (m *mockWAL) Append(msg model.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.appendErr != nil {
		return m.appendErr
	}
	m.messages = append(m.messages, msg)
	return nil
}

func (m *mockWAL) Load() ([]model.Message, error) {
	return m.messages, nil
}

func newTestBroker(wal *mockWAL) *InMemoryBroker {
	cfg := DefaultConfig()
	cfg.QueueSize = 10

	return NewInMemoryBroker(
		cfg,
		nil,
		wal,
		queue.NewRingBufferQueue(cfg.QueueSize),
		queue.NewDLQ(),
		inflight.NewManager(),
	)
}

func TestPublish(t *testing.T) {
	tests := []struct {
		name       string
		message    model.Message
		expectedID string
	}{
		{
			name:    "empty payload",
			message: model.Message{},
		},
		{
			name: "normal payload",
			message: model.Message{
				Payload: "hello",
			},
		},
		{
			name: "existing id preserved",
			message: model.Message{
				ID:      "custom-id",
				Payload: "hello",
			},
			expectedID: "custom-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wal := &mockWAL{}
			b := newTestBroker(wal)

			err := b.Publish(tt.message)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			metrics := b.Metrics()

			if metrics.ProducedCount != 1 {
				t.Fatalf("expected produced count 1, got %d", metrics.ProducedCount)
			}

			if metrics.QueueDepth != 1 {
				t.Fatalf("expected queue depth 1, got %d", metrics.QueueDepth)
			}

			if len(wal.messages) != 1 {
				t.Fatalf("expected WAL append")
			}

			msg := wal.messages[0]

			if msg.Timestamp.IsZero() {
				t.Fatal("timestamp should be populated")
			}

			if tt.expectedID != "" {
				if msg.ID != tt.expectedID {
					t.Fatalf("expected ID %q got %q", tt.expectedID, msg.ID)
				}
			} else if msg.ID == "" {
				t.Fatal("expected generated UUID")
			}
		})
	}
}

func TestPublishWALFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "append fails",
			err:  errors.New("wal append failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wal := &mockWAL{
				appendErr: tt.err,
			}

			b := newTestBroker(wal)

			err := b.Publish(model.Message{
				Payload: "hello",
			})

			if !errors.Is(err, tt.err) {
				t.Fatalf("expected %v got %v", tt.err, err)
			}

			metrics := b.Metrics()

			if metrics.ProducedCount != 0 {
				t.Fatal("message should not be published if WAL append fails")
			}

			if metrics.QueueDepth != 0 {
				t.Fatal("queue should remain empty")
			}
		})
	}
}

func TestPublishSetsCurrentTimestamp(t *testing.T) {
	wal := &mockWAL{}
	b := newTestBroker(wal)

	before := time.Now()

	err := b.Publish(model.Message{
		Payload: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}

	after := time.Now()

	ts := wal.messages[0].Timestamp

	if ts.Before(before) || ts.After(after) {
		t.Fatalf("timestamp %v not between %v and %v", ts, before, after)
	}
}

func TestBrokerRecoveryFromMessages(t *testing.T) {
	msgs := []model.Message{
		{
			ID:      "1",
			Payload: "one",
		},
		{
			ID:      "2",
			Payload: "two",
		},
		{
			ID:      "3",
			Payload: "three",
		},
	}

	cfg := DefaultConfig()

	b := NewInMemoryBroker(
		cfg,
		msgs,
		&mockWAL{},
		queue.NewRingBufferQueue(10),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	metrics := b.Metrics()

	if metrics.ProducedCount != 3 {
		t.Fatalf("expected produced count 3 got %d", metrics.ProducedCount)
	}

	if metrics.QueueDepth != 3 {
		t.Fatalf("expected queue depth 3 got %d", metrics.QueueDepth)
	}
}
