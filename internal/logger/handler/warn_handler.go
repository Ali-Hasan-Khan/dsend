package handler

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type WarnHandler struct{}

func (ih *WarnHandler) CanHandle(level model.LogLevel) bool {
	return level == model.WARN
}
