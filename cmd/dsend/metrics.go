package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ali-Hasan-Khan/dsend/client"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
)

func runMetrics(args []string) error {
	metricsCmd := flag.NewFlagSet("metrics", flag.ExitOnError)
	queueName := metricsCmd.String("queue", "", "target queue")
	metricsCmd.Parse(args)

	c, err := client.NewProducer("localhost:8080")
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *queueName != "" {
		metric, err := c.QueueMetrics(ctx, *queueName)
		if err != nil {
			return err
		}
		printQueueMetrics(*metric)
		return nil
	}

	metrics, err := c.Metrics(ctx)
	if err != nil {
		return err
	}

	for _, metric := range metrics.Queues {
		printQueueMetrics(metric)
	}

	return nil
}

func printQueueMetrics(queueMetric model.QueueMetric) {
	metric := queueMetric.Metric
	fmt.Printf("\nQueue: %s\n\nProducedCount: %d\nQueueDepth: %d\nInflightCount: %d\nDlqCount: %d\nConsumerSessionCount: %d\nAckedCount: %d\nRedeliveredCount: %d\n",
		queueMetric.Name,
		metric.ProducedCount,
		metric.QueueDepth,
		metric.InflightCount,
		metric.DlqCount,
		metric.ConsumerSessionCount,
		metric.AckedCount,
		metric.RedeliveredCount,
	)
}
