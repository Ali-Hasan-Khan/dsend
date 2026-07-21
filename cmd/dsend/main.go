package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Error: expected 'server', 'publish', 'subscribe', or 'metrics' subcommand")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "server":
		if err := runServer(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "publish":
		if err := runPublish(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "subscribe":
		if err := runSubscribe(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "metrics":
		if err := runMetrics(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Printf("Unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
}
