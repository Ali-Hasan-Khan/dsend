package logger

import (
	"github.com/Ali-Hasan-Khan/dsend/internal/logger/appender"
	"github.com/Ali-Hasan-Khan/dsend/internal/logger/handler"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

var (
	errFilter  = &handler.ErrorHandler{}
	warnFilter = &handler.WarnHandler{}
	infoFilter = &handler.InfoHandler{}
)

type LogHandlerConfiguration struct {
	errHandler  *handler.LogHandler
	warnHandler *handler.LogHandler
	infoHandler *handler.LogHandler
}

func NewLogHandlerConfiguration() *LogHandlerConfiguration {
	errH := handler.NewLogHandler(errFilter, nil)
	warnH := handler.NewLogHandler(warnFilter, errH)
	infoH := handler.NewLogHandler(infoFilter, warnH)

	return &LogHandlerConfiguration{
		errHandler:  errH,
		warnHandler: warnH,
		infoHandler: infoH,
	}
}

func (lc *LogHandlerConfiguration) Build() *handler.LogHandler {
	return lc.infoHandler
}

func (lc *LogHandlerConfiguration) AddAppenderForLevel(level model.LogLevel, appender appender.LogAppender) {
	switch level {
	case model.INFO:
		lc.infoHandler.Subscribe(appender)
	case model.WARN:
		lc.warnHandler.Subscribe(appender)
	case model.ERROR:
		lc.errHandler.Subscribe(appender)
	}
}
