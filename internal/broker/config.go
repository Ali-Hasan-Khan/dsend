package broker

import "time"

type Config struct {
	AckTimeout time.Duration
	MaxRetries int
	QueueSize  int
}

func DefaultConfig() Config {
	return Config{
		AckTimeout: time.Second * 10,
		MaxRetries: 3,
		QueueSize:  100,
	}
}
