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
	subscribeCmd.Parse(args)

	fmt.Println("Initializing subscription client...")
	c, err := client.NewConsumer("localhost:8080")
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err = c.Subscribe(); err != nil {
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
