package queue

import (
	"testing"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

func newMessage(id string) model.Message {
	return model.Message{
		ID: id,
	}
}

func TestRingBufferPushPop(t *testing.T) {
	q := NewRingBufferQueue(5)

	q.Push(newMessage("1"))
	q.Push(newMessage("2"))
	q.Push(newMessage("3"))

	if q.Size() != 3 {
		t.Fatalf("expected size 3, got %d", q.Size())
	}

	msg := q.Pop()
	if msg.ID != "1" {
		t.Fatalf("expected first message to be 1, got %s", msg.ID)
	}

	msg = q.Pop()
	if msg.ID != "2" {
		t.Fatalf("expected second message to be 2, got %s", msg.ID)
	}

	msg = q.Pop()
	if msg.ID != "3" {
		t.Fatalf("expected third message to be 3, got %s", msg.ID)
	}

	if q.Size() != 0 {
		t.Fatalf("expected queue to be empty")
	}
}

func TestRingBufferPeek(t *testing.T) {
	q := NewRingBufferQueue(5)

	q.Push(newMessage("1"))
	q.Push(newMessage("2"))

	msg := q.Peek()

	if msg.ID != "1" {
		t.Fatalf("expected peek to return first element")
	}

	if q.Size() != 2 {
		t.Fatalf("peek should not remove item")
	}
}

func TestRingBufferFIFO(t *testing.T) {
	q := NewRingBufferQueue(10)

	for i := 0; i < 10; i++ {
		q.Push(newMessage(string(rune('A' + i))))
	}

	for i := 0; i < 10; i++ {
		msg := q.Pop()

		expected := string(rune('A' + i))

		if msg.ID != expected {
			t.Fatalf("expected %s got %s", expected, msg.ID)
		}
	}
}

func TestRingBufferWrapAround(t *testing.T) {
	q := NewRingBufferQueue(3)

	q.Push(newMessage("1"))
	q.Push(newMessage("2"))
	q.Push(newMessage("3"))

	if q.Pop().ID != "1" {
		t.Fatal("expected 1")
	}

	q.Push(newMessage("4"))

	if q.Pop().ID != "2" {
		t.Fatal("expected 2")
	}

	if q.Pop().ID != "3" {
		t.Fatal("expected 3")
	}

	if q.Pop().ID != "4" {
		t.Fatal("expected 4")
	}

	if q.Size() != 0 {
		t.Fatal("queue should be empty")
	}
}

func TestRingBufferCapacity(t *testing.T) {
	q := NewRingBufferQueue(20)

	if q.Capacity() != 20 {
		t.Fatalf("expected capacity 20 got %d", q.Capacity())
	}
}

func TestRingBufferSize(t *testing.T) {
	q := NewRingBufferQueue(5)

	if q.Size() != 0 {
		t.Fatal("new queue should be empty")
	}

	q.Push(newMessage("1"))

	if q.Size() != 1 {
		t.Fatal("size should be 1")
	}

	q.Push(newMessage("2"))

	if q.Size() != 2 {
		t.Fatal("size should be 2")
	}

	q.Pop()

	if q.Size() != 1 {
		t.Fatal("size should be 1 after pop")
	}
}
