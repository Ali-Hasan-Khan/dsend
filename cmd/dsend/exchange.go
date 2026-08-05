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

func runExchange(args []string) error {
	exchangeCmd := flag.NewFlagSet("exchange", flag.ExitOnError)
	exchangeCmd.Parse(args)
	remainingArgs := exchangeCmd.Args()
	if len(remainingArgs) < 1 {
		return errors.New("Error: expected 'create', 'delete', or 'list' subcommand")
	}

	cmd := remainingArgs[0]

	switch cmd {
	case "create":
		if len(remainingArgs) < 3 {
			return errors.New("Error: missing required <name> and <type> arguments")
		}
		exchangeName := remainingArgs[1]
		exchangeType := remainingArgs[2]
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		err = c.CreateExchange(ctx, exchangeName, exchangeType)
		if err != nil {
			return err
		}

		fmt.Println("Exchange created successfully")
	case "delete":
		if len(remainingArgs) < 2 {
			return errors.New("Error: missing required <name> argument")
		}

		exchangeName := strings.Join(remainingArgs[1:], " ")
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		err = c.DeleteExchange(ctx, exchangeName)
		if err != nil {
			return err
		}

		fmt.Println("Exchange deleted successfully")
	case "list":
		c, err := client.NewProducer("localhost:8080")
		if err != nil {
			return err
		}
		defer c.Close()

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		exchanges, err := c.ListExchanges(ctx)
		if err != nil {
			return err
		}

		fmt.Println("Exchanges:", exchanges)
	default:
		return errors.New("Error: invalid argument")
	}

	return nil
}
