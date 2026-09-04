package handler

import (
	"github.com/Ali-Hasan-Khan/dsend/internal/logger/appender"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type LogFilter interface {
	CanHandle(level model.LogLevel) bool
}

type LogHandler struct {
	next      *LogHandler
	appenders []appender.LogAppender
	filter    LogFilter
}

func NewLogHandler(filter LogFilter, next *LogHandler) *LogHandler {
	return &LogHandler{
		filter:    filter,
		next:      next,
		appenders: make([]appender.LogAppender, 0),
	}
}

func (lh *LogHandler) Subscribe(observer appender.LogAppender) {
	lh.appenders = append(lh.appenders, observer)
}

func (lh *LogHandler) NotifyObservers(message model.LogMessage) {
	for _, appender := range lh.appenders {
		_ = appender.Append(message)
	}
}

func (lh *LogHandler) Handle(message model.LogMessage) {
	if lh.filter.CanHandle(message.Level) {
		lh.NotifyObservers(message)
	}
	if lh.next != nil {
		lh.next.Handle(message)
	}
}
