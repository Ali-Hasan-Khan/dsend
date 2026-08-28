package handler

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type ErrorHandler struct{}

func (ih *ErrorHandler) CanHandle(level model.LogLevel) bool {
	return level == model.ERROR
}
