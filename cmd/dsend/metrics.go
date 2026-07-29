package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ali-Hasan-Khan/dsend/client"
)

func runMetrics(args []string) error {
	metricsCmd := flag.NewFlagSet("metrics", flag.ExitOnError)
	metricsCmd.Parse(args)

	c, err := client.NewProducer("localhost:8080")
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resp, err := c.Metrics(ctx)
	if err != nil {
		return err
	}

	for _, r := range resp.Queues {
		m := r.Metric
		fmt.Printf("ProducedCount: %v\nQueueDepth: %v\nInflightCount: %v\nDlqCount: %v\nConsumerSessionCount: %v\nAckedCount: %v\nRedeliveredCount: %v\n",
			m.ProducedCount, m.QueueDepth,
			m.InflightCount, m.DlqCount,
			m.ConsumerSessionCount, m.AckedCount,
			m.RedeliveredCount,
		)
	}

	c.Close()
	return nil
}
