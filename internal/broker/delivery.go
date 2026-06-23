package broker

import "github.com/Ali-Hasan-Khan/dsend/internal/model"

type Delivery struct {
	model.Message
	AckToken string
}
