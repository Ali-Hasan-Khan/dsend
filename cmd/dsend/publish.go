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

func runPublish(args []string) error {
	publishCmd := flag.NewFlagSet("publish", flag.ExitOnError)
	exchangeName := publishCmd.String("exchange", "default", "target exchange")
	publishCmd.Parse(args)
	remainingArgs := publishCmd.Args()

	if len(remainingArgs) < 2 {
		return errors.New("Error: missing required <routingKey> and <payload> arguments")
	}

	routingKey := remainingArgs[0]
	payload := strings.Join(remainingArgs[1:], " ")
	c, err := client.NewProducer("localhost:8080")
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = c.Publish(ctx, *exchangeName, routingKey, payload)
	if err != nil {
		return err
	}

	fmt.Println("Message Sent successfully")

	return nil
}
