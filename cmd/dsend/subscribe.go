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

func runSubscribe(args []string) error {
	subscribeCmd := flag.NewFlagSet("subscribe", flag.ExitOnError)
	queueName := subscribeCmd.String("queue", "default", "target queue")
	addr := subscribeCmd.String("addr", "127.0.0.1:8080", "server address")
	if err := subscribeCmd.Parse(args); err != nil {
		return err
	}

	fmt.Println("Initializing subscription client...")
	c, err := client.NewConsumer(*addr)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err = c.Subscribe(*queueName); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := c.Receive(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("Received message: %s\n", msg.Payload)
		if err := c.Ack(msg.AckToken); err != nil {
			return err
		}
	}
}
