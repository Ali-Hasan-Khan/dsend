package storage

import (
	"os"
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

func published(queue string, message model.Message) model.Record {
	return model.Record{
		Type:      model.Published,
		Queue:     queue,
		Message:   message,
		MessageID: message.ID,
	}
}

func recoveredMessages(t *testing.T, state RecoveredState, queue string) []model.Message {
	t.Helper()

	messages, ok := state.PendingMessages[queue]
	if !ok {
		return nil
	}

	return messages
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

	state, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.PendingMessages) != 0 {
		t.Fatalf("expected empty WAL, got %d queues", len(state.PendingMessages))
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

	if err := wal.Append(published(model.DefaultQueueName, msg)); err != nil {
		t.Fatal(err)
	}

	state, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	messages := recoveredMessages(t, state, model.DefaultQueueName)
	if len(messages) != 1 {
		t.Fatalf("expected 1 message got %d", len(messages))
	}

	if messages[0].ID != "1" {
		t.Fatalf("expected ID=1 got %s", messages[0].ID)
	}

	if messages[0].Payload != "hello" {
		t.Fatalf("expected payload=hello got %s", messages[0].Payload)
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
		if err := wal.Append(published(model.DefaultQueueName, msg)); err != nil {
			t.Fatal(err)
		}
	}

	state, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	messages := recoveredMessages(t, state, model.DefaultQueueName)
	if len(messages) != len(tests) {
		t.Fatalf("expected %d messages got %d", len(tests), len(messages))
	}

	for i := range tests {
		if messages[i].ID != tests[i].ID {
			t.Fatalf("message %d: expected ID=%s got %s",
				i,
				tests[i].ID,
				messages[i].ID,
			)
		}

		if messages[i].Payload != tests[i].Payload {
			t.Fatalf("message %d: expected payload=%s got %s",
				i,
				tests[i].Payload,
				messages[i].Payload,
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

		if err := wal.Append(published(model.DefaultQueueName, newMessage("1", "hello"))); err != nil {
			t.Fatal(err)
		}

		if err := wal.Append(published(model.DefaultQueueName, newMessage("2", "world"))); err != nil {
			t.Fatal(err)
		}
	}

	{
		wal, err := NewFileWAL(path)
		if err != nil {
			t.Fatal(err)
		}

		state, err := wal.Load()
		if err != nil {
			t.Fatal(err)
		}

		messages := recoveredMessages(t, state, model.DefaultQueueName)
		if len(messages) != 2 {
			t.Fatalf("expected 2 recovered messages got %d", len(messages))
		}

		if messages[0].ID != "1" {
			t.Fatalf("expected first ID=1 got %s", messages[0].ID)
		}

		if messages[1].ID != "2" {
			t.Fatalf("expected second ID=2 got %s", messages[1].ID)
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

		if err := wal.Append(published(model.DefaultQueueName, msg)); err != nil {
			t.Fatal(err)
		}
	}

	state, err := wal.Load()
	if err != nil {
		t.Fatal(err)
	}

	messages := recoveredMessages(t, state, model.DefaultQueueName)
	if len(messages) != 100 {
		t.Fatalf("expected 100 messages got %d", len(messages))
	}

	for i := 0; i < 100; i++ {
		expected := string(rune(i))

		if messages[i].ID != expected {
			t.Fatalf(
				"order mismatch at %d expected %q got %q",
				i,
				expected,
				messages[i].ID,
			)
		}
	}
}

func TestLoadCorruptedWAL(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "wal.log")

	err := os.WriteFile(
		path,
		[]byte("{bad json}\n"),
		0644,
	)
	if err != nil {
		t.Fatal(err)
	}

	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	_, err = wal.Load()

	if err == nil {
		t.Fatal("expected decode error")
	}
}
