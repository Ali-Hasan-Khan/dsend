package formatter

import (
	"fmt"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type PlainTextFormatter struct {
}

func NewPlainTextFormatter() *PlainTextFormatter {
	return &PlainTextFormatter{}
}

func (f *PlainTextFormatter) Format(m model.LogMessage) string {
	return fmt.Sprintf("%v [%v] - %v", m.Timestamp.Format("2006-01-02 15:04:05"), m.Level, m.Message)
}
