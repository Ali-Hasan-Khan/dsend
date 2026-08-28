package handler

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type InfoHandler struct{}

func (ih *InfoHandler) CanHandle(level model.LogLevel) bool {
	return level == model.INFO
}
