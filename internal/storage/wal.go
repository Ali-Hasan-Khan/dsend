package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type WAL interface {
	Append(record model.Record) error
	Load() (RecoveredState, error)
}

type RecoveredState struct {
	PendingMessages map[string][]model.Message
}

type FileWAL struct {
	path string
}

func NewFileWAL(path string) (*FileWAL, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL file: %w", err)
	}
	file.Close()

	return &FileWAL{path: path}, nil
}

func (f *FileWAL) Append(record model.Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("error marshalling data: %w", err)
	}

	file, err := os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("error appending to file: %w", err)
	}
	return nil
}

func removeMessage(messages []model.Message, messageID string) []model.Message {
	return slices.DeleteFunc(messages, func(message model.Message) bool {
		return message.ID == messageID
	})
}

func (f *FileWAL) Load() (RecoveredState, error) {
	file, err := os.Open(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RecoveredState{}, nil
		}
		return RecoveredState{}, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	state := RecoveredState{
		PendingMessages: make(map[string][]model.Message),
	}

	decoder := json.NewDecoder(file)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return RecoveredState{}, fmt.Errorf("error decoding log entry: %w", err)
		}

		var record model.Record
		if err := json.Unmarshal(raw, &record); err != nil {
			return RecoveredState{}, fmt.Errorf("error decoding WAL record: %w", err)
		}

		if record.Type == "" {
			var message model.Message
			if err := json.Unmarshal(raw, &message); err != nil {
				return RecoveredState{}, fmt.Errorf("error decoding WAL record: %w", err)
			}

			state.PendingMessages[model.DefaultQueueName] = append(
				state.PendingMessages[model.DefaultQueueName],
				message,
			)
			continue
		}

		switch record.Type {
		case model.QueueCreated:
			if _, exists := state.PendingMessages[record.Queue]; !exists {
				state.PendingMessages[record.Queue] = nil
			}
		case model.Published:
			state.PendingMessages[record.Queue] = append(
				state.PendingMessages[record.Queue],
				record.Message,
			)
		case model.Requeued:
			messages := removeMessage(
				state.PendingMessages[record.Queue],
				record.MessageID,
			)
			state.PendingMessages[record.Queue] = append(messages, record.Message)
		case model.Acknowledged, model.DeadLettered:
			state.PendingMessages[record.Queue] = removeMessage(
				state.PendingMessages[record.Queue],
				record.MessageID,
			)
		case model.QueueDeleted:
			delete(state.PendingMessages, record.Queue)
		}
	}

	return state, nil
}
