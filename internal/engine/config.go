package engine

import "time"

type Config struct {
	AckTimeout         time.Duration
	RedeliveryInterval time.Duration
	ExpiryInterval     time.Duration
	MaxRetries         int
	QueueSize          int
	ConsumerPrefetch   int
}

func DefaultConfig() Config {
	return Config{
		AckTimeout:         time.Second * 100,
		RedeliveryInterval: 5 * time.Second,
		ExpiryInterval:     1 * time.Second,
		MaxRetries:         3,
		QueueSize:          20,
		ConsumerPrefetch:   10,
	}
}
