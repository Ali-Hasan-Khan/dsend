package logger

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/logger/appender"
	"github.com/Ali-Hasan-Khan/dsend/internal/logger/formatter"
	"github.com/Ali-Hasan-Khan/dsend/internal/logger/handler"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type Logger struct {
	cfg          *LogHandlerConfiguration
	handlerChain *handler.LogHandler
}

var (
	instance *Logger
	once     sync.Once
)

func GetInstance() *Logger {
	once.Do(func() {
		config := NewLogHandlerConfiguration()

		console := appender.NewConsoleAppender(formatter.NewPlainTextFormatter())
		config.AddAppenderForLevel(model.INFO, console)
		config.AddAppenderForLevel(model.WARN, console)
		config.AddAppenderForLevel(model.ERROR, console)

		file, err := appender.NewFileAppender("./data/dsend.log", formatter.NewPlainTextFormatter())
		if err == nil {
			config.AddAppenderForLevel(model.WARN, file)
			config.AddAppenderForLevel(model.ERROR, file)
		}

		instance = &Logger{
			cfg:          config,
			handlerChain: config.Build(),
		}

		if err != nil {
			instance.Warnf("file logging disabled: %v", err)
		}
	})
	return instance
}

func (l *Logger) log(level model.LogLevel, message string) {
	msg := model.LogMessage{
		Level:     level,
		Message:   message,
		Timestamp: time.Now(),
	}
	l.handlerChain.Handle(msg)
}

func (l *Logger) Info(message string) {
	l.log(model.INFO, message)
}

func (l *Logger) Warn(message string) {
	l.log(model.WARN, message)
}

func (l *Logger) Error(message string) {
	l.log(model.ERROR, message)
}

func (l *Logger) Fatal(message string) {
	l.Error(message)
	os.Exit(1)
}

func (l *Logger) Infof(format string, args ...any) {
	l.log(model.INFO, fmt.Sprintf(format, args...))
}

func (l *Logger) Warnf(format string, args ...any) {
	l.log(model.WARN, fmt.Sprintf(format, args...))
}

func (l *Logger) Errorf(format string, args ...any) {
	l.log(model.ERROR, fmt.Sprintf(format, args...))
}

func (l *Logger) Fatalf(format string, args ...any) {
	l.Errorf(format, args...)
	os.Exit(1)
}
