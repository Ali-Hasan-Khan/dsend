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
	if len(remainingArgs) < 1 {
		return errors.New("Error: expected 'create', 'delete', or 'list' subcommand")
	}

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
	case "bind":
		if len(remainingArgs) < 4 {
			return errors.New("Error: missing required <exchangeName>,<queueName>, and <bindingKey> arguments")
		}

		exchangeName := remainingArgs[1]
		queueName := remainingArgs[2]
		bindingKey := strings.Join(remainingArgs[3:], " ")
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		err = c.BindQueue(ctx, exchangeName, queueName, bindingKey)
		if err != nil {
			return err
		}

		fmt.Println("Queue bounded successfully")
	case "unbind":
		if len(remainingArgs) < 4 {
			return errors.New("Error: missing required <exchangeName>,<queueName>, and <bindingKey> arguments")
		}

		exchangeName := remainingArgs[1]
		queueName := remainingArgs[2]
		bindingKey := strings.Join(remainingArgs[3:], " ")
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		err = c.UnbindQueue(ctx, exchangeName, queueName, bindingKey)
		if err != nil {
			return err
		}

		fmt.Println("Queue unbounded successfully")
	default:
		return errors.New("Error: invalid argument")
	}

	return nil
}
