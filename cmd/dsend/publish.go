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
	publishCmd.Parse(args)
	remainingArgs := publishCmd.Args()

	if len(remainingArgs) < 1 {
		return errors.New("Error: missing required <message> argument")
	}

	payload := strings.Join(remainingArgs, " ")
	c, err := client.NewProducer("localhost:8080")
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = c.Publish(ctx, payload)
	if err != nil {
		return err
	}

	fmt.Println("Message Sent successfully")

	return nil
}
