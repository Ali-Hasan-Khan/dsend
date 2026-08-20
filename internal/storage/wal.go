package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type WAL interface {
	Append(record model.Record) error
	Load() (RecoveredState, error)
	Close() error
}

type ExchangeState struct {
	ExchangeType string
	Bindings     []model.Binding
}

type RecoveredState struct {
	PendingMessages  map[string][]model.Message
	PendingExchanges map[string]*ExchangeState
}

type appendRequest struct {
	record model.Record
	result chan error
}

type FileWAL struct {
	path string
	file *os.File

	closed   bool
	wg       sync.WaitGroup
	mu       sync.Mutex
	appendCh chan appendRequest
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

	wal := &FileWAL{
		path:     path,
		file:     file,
		appendCh: make(chan appendRequest),
	}

	wal.wg.Add(1)
	go wal.runWriter()

	return wal, nil
}

func (f *FileWAL) writeRecord(record model.Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("error marshalling data: %w", err)
	}

	data = append(data, '\n')

	n, err := f.file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write record to WAL file: %w", err)
	}

	if n != len(data) {
		return io.ErrShortWrite
	}

	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file to disk: %w", err)
	}

	return nil
}

func (f *FileWAL) runWriter() {
	defer f.wg.Done()

	for req := range f.appendCh {
		err := f.writeRecord(req.record)
		req.result <- err
	}
}

func (f *FileWAL) Append(record model.Record) error {
	result := make(chan error, 1)

	req := appendRequest{
		record: record,
		result: result,
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return errors.New("WAL closed")
	}

	f.appendCh <- req
	f.mu.Unlock()

	return <-result
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
		PendingMessages:  make(map[string][]model.Message),
		PendingExchanges: map[string]*ExchangeState{},
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
		case model.QueueBinded:
			exchangeState, ok := state.PendingExchanges[record.Exchange]
			if !ok {
				continue
			}
			exchangeState.Bindings = append(
				exchangeState.Bindings, model.Binding{
					QueueName:  record.Queue,
					BindingKey: record.BindingKey,
				},
			)
		case model.QueueUnbinded:
			exchangeState, ok := state.PendingExchanges[record.Exchange]
			if !ok {
				continue
			}
			exchangeState.Bindings = slices.DeleteFunc(
				exchangeState.Bindings,
				func(b model.Binding) bool {
					if record.BindingKey != "" {
						return b.BindingKey == record.BindingKey && b.QueueName == record.Queue
					}
					return b.QueueName == record.Queue
				},
			)
		case model.ExchangeCreated:
			state.PendingExchanges[record.Exchange] = &ExchangeState{
				ExchangeType: record.ExchangeType,
				Bindings:     nil,
			}
		case model.ExchangeDeleted:
			delete(state.PendingExchanges, record.Exchange)
		}
	}

	return state, nil
}

func (f *FileWAL) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}

	f.closed = true
	close(f.appendCh)
	f.mu.Unlock()

	f.wg.Wait()

	return f.file.Close()
}
