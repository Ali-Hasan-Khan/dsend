package formatter

import (
	"encoding/json"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

type JsonFormatter struct {
}

func NewJsonFormatter() *JsonFormatter {
	return &JsonFormatter{}
}

func (f *JsonFormatter) Format(message model.LogMessage) string {
	bytes, err := json.Marshal(message)
	if err != nil {
		return `{"error": "failed to marshal log message"}`
	}
	return string(bytes)
}
