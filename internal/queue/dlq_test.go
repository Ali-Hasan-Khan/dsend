package queue

import (
	"testing"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

func TestDLQPush(t *testing.T) {
	dlq := NewDLQ()

	dlq.Push(model.Message{
		ID: "1",
	})

	if dlq.Size() != 1 {
		t.Fatalf("expected size 1 got %d", dlq.Size())
	}
}

func TestDLQPeek(t *testing.T) {
	dlq := NewDLQ()

	dlq.Push(model.Message{
		ID: "1",
	})

	msg := dlq.Peek()

	if msg.ID != "1" {
		t.Fatalf("expected id=1 got=%s", msg.ID)
	}

	if dlq.Size() != 1 {
		t.Fatal("peek should not remove message")
	}
}

func TestDLQSize(t *testing.T) {
	dlq := NewDLQ()

	if dlq.Size() != 0 {
		t.Fatal("new dlq should be empty")
	}

	dlq.Push(model.Message{ID: "1"})
	dlq.Push(model.Message{ID: "2"})
	dlq.Push(model.Message{ID: "3"})

	if dlq.Size() != 3 {
		t.Fatalf("expected size 3 got %d", dlq.Size())
	}
}

func TestDLQPushMultiple(t *testing.T) {
	dlq := NewDLQ()

	for i := 0; i < 100; i++ {
		dlq.Push(model.Message{
			ID: string(rune(i)),
		})
	}

	if dlq.Size() != 100 {
		t.Fatalf("expected 100 got %d", dlq.Size())
	}

	if dlq.Peek().ID != string(rune(99)) {
		t.Fatal("peek should return latest message")
	}
}
