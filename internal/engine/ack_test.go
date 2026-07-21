package engine

import (
	"errors"
	"testing"

	"github.com/Ali-Hasan-Khan/dsend/internal/inflight"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/queue"
)

func TestAck(t *testing.T) {
	tests := []struct {
		name         string
		token        string
		setup        func(*InMemoryBroker)
		wantErr      bool
		wantAcked    int
		wantInflight int
	}{
		{
			name:  "valid ack",
			token: "token-1",
			setup: func(b *InMemoryBroker) {
				b.inFlightManager.Add("token-1", model.Message{
					ID: "msg-1",
				})
			},
			wantAcked:    1,
			wantInflight: 0,
		},
		{
			name:         "invalid token",
			token:        "missing",
			setup:        func(*InMemoryBroker) {},
			wantErr:      true,
			wantAcked:    0,
			wantInflight: 0,
		},
		{
			name:  "duplicate ack",
			token: "token-2",
			setup: func(b *InMemoryBroker) {
				b.inFlightManager.Add("token-2", model.Message{
					ID: "msg-2",
				})

				if err := b.Ack("token-2"); err != nil {
					t.Fatalf("unexpected setup error: %v", err)
				}
			},
			wantErr:      true,
			wantAcked:    1,
			wantInflight: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			wal := &mockWAL{}

			b := NewInMemoryBroker(
				DefaultConfig(),
				nil,
				wal,
				queue.NewRingBufferQueue(10),
				queue.NewDLQ(),
				inflight.NewManager(),
			)

			tt.setup(b)

			err := b.Ack(tt.token)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}

				if !errors.Is(err, ErrInvalidAckToken) {
					t.Fatalf("unexpected error: %v", err)
				}

			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			metrics := b.Metrics()

			if metrics.AckedCount != tt.wantAcked {
				t.Fatalf(
					"expected ack count %d got %d",
					tt.wantAcked,
					metrics.AckedCount,
				)
			}

			if metrics.InflightCount != tt.wantInflight {
				t.Fatalf(
					"expected inflight %d got %d",
					tt.wantInflight,
					metrics.InflightCount,
				)
			}
		})
	}
}

func TestAckDoesNotChangeProducedCount(t *testing.T) {
	b := NewInMemoryBroker(
		DefaultConfig(),
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(10),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	b.producedCount = 5

	b.inFlightManager.Add("token", model.Message{})

	if err := b.Ack("token"); err != nil {
		t.Fatal(err)
	}

	if got := b.Metrics().ProducedCount; got != 5 {
		t.Fatalf("expected produced count 5 got %d", got)
	}
}

func TestAckDoesNotChangeQueueDepth(t *testing.T) {
	b := NewInMemoryBroker(
		DefaultConfig(),
		nil,
		&mockWAL{},
		queue.NewRingBufferQueue(10),
		queue.NewDLQ(),
		inflight.NewManager(),
	)

	b.queue.Push(model.Message{ID: "1"})
	b.queue.Push(model.Message{ID: "2"})

	b.inFlightManager.Add("token", model.Message{})

	before := b.Metrics().QueueDepth

	if err := b.Ack("token"); err != nil {
		t.Fatal(err)
	}

	after := b.Metrics().QueueDepth

	if before != after {
		t.Fatalf(
			"queue depth changed from %d to %d",
			before,
			after,
		)
	}
}
