package inflight

import (
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

func newMessage(id string) model.Message {
	return model.Message{
		ID: id,
	}
}

func TestManagerAddAndIsPresent(t *testing.T) {
	tests := []struct {
		name  string
		token string
		msgID string
	}{
		{
			name:  "single message",
			token: "token-1",
			msgID: "1",
		},
		{
			name:  "another message",
			token: "token-2",
			msgID: "2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager()

			m.Add(tt.token, newMessage(tt.msgID))

			if !m.IsPresent(tt.token) {
				t.Fatalf("expected token %s to be present", tt.token)
			}

			if m.Size() != 1 {
				t.Fatalf("expected size 1 got %d", m.Size())
			}
		})
	}
}

func TestManagerRemove(t *testing.T) {
	m := NewManager()

	m.Add("abc", newMessage("1"))

	if !m.IsPresent("abc") {
		t.Fatal("token should exist")
	}

	m.Remove("abc")

	if m.IsPresent("abc") {
		t.Fatal("token should have been removed")
	}

	if m.Size() != 0 {
		t.Fatalf("expected empty manager got %d", m.Size())
	}
}

func TestManagerRemoveInvalidToken(t *testing.T) {
	m := NewManager()

	m.Add("abc", newMessage("1"))

	m.Remove("does-not-exist")

	if !m.IsPresent("abc") {
		t.Fatal("removing invalid token should not affect existing entries")
	}

	if m.Size() != 1 {
		t.Fatalf("expected size 1 got %d", m.Size())
	}
}

func TestManagerExpired(t *testing.T) {
	tests := []struct {
		name           string
		sleep          time.Duration
		timeout        time.Duration
		expectedExpiry int
	}{
		{
			name:           "not expired",
			sleep:          5 * time.Millisecond,
			timeout:        20 * time.Millisecond,
			expectedExpiry: 0,
		},
		{
			name:           "expired",
			sleep:          30 * time.Millisecond,
			timeout:        10 * time.Millisecond,
			expectedExpiry: 1,
		},
		{
			name:           "equal timeout",
			sleep:          20 * time.Millisecond,
			timeout:        20 * time.Millisecond,
			expectedExpiry: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager()

			m.Add("abc", newMessage("1"))

			time.Sleep(tt.sleep)

			expired := m.Expired(tt.timeout)

			if len(expired) != tt.expectedExpiry {
				t.Fatalf(
					"expected %d expired messages got %d",
					tt.expectedExpiry,
					len(expired),
				)
			}
		})
	}
}

func TestManagerMultipleExpiredMessages(t *testing.T) {
	m := NewManager()

	m.Add("1", newMessage("1"))
	m.Add("2", newMessage("2"))
	m.Add("3", newMessage("3"))

	time.Sleep(25 * time.Millisecond)

	expired := m.Expired(10 * time.Millisecond)

	if len(expired) != 3 {
		t.Fatalf("expected 3 expired messages got %d", len(expired))
	}
}

func TestManagerSize(t *testing.T) {
	m := NewManager()

	if m.Size() != 0 {
		t.Fatal("new manager should be empty")
	}

	m.Add("1", newMessage("1"))
	m.Add("2", newMessage("2"))
	m.Add("3", newMessage("3"))

	if m.Size() != 3 {
		t.Fatalf("expected size 3 got %d", m.Size())
	}

	m.Remove("2")

	if m.Size() != 2 {
		t.Fatalf("expected size 2 got %d", m.Size())
	}
}
