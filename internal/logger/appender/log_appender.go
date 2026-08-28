package appender

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type LogAppender interface {
	Append(message model.LogMessage) error
}
