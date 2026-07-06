package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

func newMessage(id, payload string) model.Message {
	return model.Message{
		ID:        id,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}
}

func TestNewFileWALCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	_, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
}

func TestLoadEmptyWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 0 {
		t.Fatalf("expected empty WAL, got %d messages", len(msgs))
	}
}

func TestAppendSingleMessage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	msg := newMessage("1", "hello")

	if err := wal.Append(msg); err != nil {
		t.Fatal(err)
	}

	msgs, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message got %d", len(msgs))
	}

	if msgs[0].ID != "1" {
		t.Fatalf("expected ID=1 got %s", msgs[0].ID)
	}

	if msgs[0].Payload != "hello" {
		t.Fatalf("expected payload=hello got %s", msgs[0].Payload)
	}
}

func TestAppendMultipleMessages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	tests := []model.Message{
		newMessage("1", "hello"),
		newMessage("2", "world"),
		newMessage("3", "golang"),
		newMessage("4", "broker"),
	}

	for _, msg := range tests {
		if err := wal.Append(msg); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != len(tests) {
		t.Fatalf("expected %d messages got %d", len(tests), len(msgs))
	}

	for i := range tests {
		if msgs[i].ID != tests[i].ID {
			t.Fatalf("message %d: expected ID=%s got %s",
				i,
				tests[i].ID,
				msgs[i].ID,
			)
		}

		if msgs[i].Payload != tests[i].Payload {
			t.Fatalf("message %d: expected payload=%s got %s",
				i,
				tests[i].Payload,
				msgs[i].Payload,
			)
		}
	}
}

func TestRecoveryAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	{
		wal, err := NewFileWAL(path)
		if err != nil {
			t.Fatal(err)
		}

		if err := wal.Append(newMessage("1", "hello")); err != nil {
			t.Fatal(err)
		}

		if err := wal.Append(newMessage("2", "world")); err != nil {
			t.Fatal(err)
		}
	}

	{
		wal, err := NewFileWAL(path)
		if err != nil {
			t.Fatal(err)
		}

		msgs, err := wal.Load()
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 2 {
			t.Fatalf("expected 2 recovered messages got %d", len(msgs))
		}

		if msgs[0].ID != "1" {
			t.Fatalf("expected first ID=1 got %s", msgs[0].ID)
		}

		if msgs[1].ID != "2" {
			t.Fatalf("expected second ID=2 got %s", msgs[1].ID)
		}
	}
}

func TestAppendPreservesOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		msg := newMessage(string(rune(i)), "payload")

		if err := wal.Append(msg); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 100 {
		t.Fatalf("expected 100 messages got %d", len(msgs))
	}

	for i := 0; i < 100; i++ {
		expected := string(rune(i))

		if msgs[i].ID != expected {
			t.Fatalf(
				"order mismatch at %d expected %q got %q",
				i,
				expected,
				msgs[i].ID,
			)
		}
	}
}
