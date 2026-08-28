package formatter

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type LogFormatter interface {
	Format(message model.LogMessage) string
}
