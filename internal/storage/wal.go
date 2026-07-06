package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type WAL interface {
	Append(msg model.Message) error
	Load() ([]model.Message, error)
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

func (f *FileWAL) Append(msg model.Message) error {
	data, err := json.Marshal(msg)
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

func (f *FileWAL) Load() ([]model.Message, error) {
	file, err := os.Open(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []model.Message{}, nil
		}
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	var msgs []model.Message

	decoder := json.NewDecoder(file)
	for {
		var msg model.Message
		if err := decoder.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error decoding log entry: %w", err)
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}
