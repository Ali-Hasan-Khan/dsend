package appender

import (
	"fmt"

	"github.com/Ali-Hasan-Khan/dsend/internal/logger/formatter"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type ConsoleAppender struct {
	formatter formatter.LogFormatter
}

func NewConsoleAppender(formatter formatter.LogFormatter) *ConsoleAppender {
	return &ConsoleAppender{
		formatter: formatter,
	}
}

func (ca *ConsoleAppender) Append(message model.LogMessage) error {
	fmt.Println(ca.formatter.Format(message))
	return nil
}
