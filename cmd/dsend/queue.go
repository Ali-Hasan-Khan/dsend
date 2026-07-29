package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Ali-Hasan-Khan/dsend/client"
)

func runQueue(args []string) error {
	queueCmd := flag.NewFlagSet("queue", flag.ExitOnError)
	queueCmd.Parse(args)
	remainingArgs := queueCmd.Args()

	cmd := remainingArgs[0]

	switch cmd {
	case "create":
		if len(remainingArgs) < 2 {
			return errors.New("Error: missing required <name> argument")
		}
		queueName := strings.Join(remainingArgs[1:], " ")
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		err = c.CreateQueue(ctx, queueName)
		if err != nil {
			return err
		}

		fmt.Println("Queue created successfully")
	case "delete":
		if len(remainingArgs) < 2 {
			return errors.New("Error: missing required <name> argument")
		}

		queueName := strings.Join(remainingArgs[1:], " ")
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		err = c.DeleteQueue(ctx, queueName)
		if err != nil {
			return err
		}

		fmt.Println("Queue deleted successfully")
	case "list":
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		queues, err := c.ListQueues(ctx)
		if err != nil {
			return err
		}

		fmt.Println("Queues:", queues)
	default:
		return errors.New("Error: invalid argument")
	}

	return nil
}
