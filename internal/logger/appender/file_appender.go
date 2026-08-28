package appender

import (
	"fmt"
	"os"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/logger/formatter"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type FileAppender struct {
	mu        sync.Mutex
	formatter formatter.LogFormatter
	file      *os.File
}

func NewFileAppender(path string, f formatter.LogFormatter) (*FileAppender, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	return &FileAppender{
		formatter: f,
		file:      file,
	}, nil
}

func (fa *FileAppender) Append(message model.LogMessage) error {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if _, err := fa.file.WriteString(fa.formatter.Format(message) + "\n"); err != nil {
		return fmt.Errorf("failed writing log record: %w", err)
	}
	return nil
}

func (fa *FileAppender) Close() error {
	return fa.file.Close()
}
