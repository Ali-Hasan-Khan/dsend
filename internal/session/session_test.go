package session

import (
	"testing"
	"time"
)

func TestClose(t *testing.T) {
	cs := &ConsumerSession{
		Closed: make(chan struct{}),
	}

	cs.Close()

	select {
	case _, open := <-cs.Closed:
		if open {
			t.Error("Expected channel to be closed, but it was open")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Test timed out waiting for channel to close")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Second Close() call panicked: %v", r)
		}
	}()

	cs.Close()
}

func TestIsEligible(t *testing.T) {
	tests := []struct {
		name     string
		unacked  int
		prefetch int
		eligible bool
	}{
		{
			name:     "fresh session below limit",
			unacked:  0,
			prefetch: 10,
			eligible: true,
		},
		{
			name:     "below limit",
			unacked:  3,
			prefetch: 10,
			eligible: true,
		},
		{
			name:     "at limit",
			unacked:  10,
			prefetch: 10,
			eligible: false,
		},
		{
			name:     "above limit",
			unacked:  12,
			prefetch: 10,
			eligible: false,
		},
		{
			name:     "prefetch of one",
			unacked:  0,
			prefetch: 1,
			eligible: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewConsumerSession("consumer-1")
			cs.UnackedCount = tt.unacked

			if got := cs.IsEligible(tt.prefetch); got != tt.eligible {
				t.Fatalf("expected eligible=%v got %v", tt.eligible, got)
			}
		})
	}
}
