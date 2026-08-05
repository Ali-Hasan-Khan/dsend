package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Error: expected 'server', 'publish', 'subscribe', 'queue create/delete/list', 'metrics', or 'version' subcommand")
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
	case "queue":
		if err := runQueue(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "version":
		fmt.Printf("dsend %s\ncommit: %s\nbuilt: %s\ngo: %s\n", Version, Commit, BuildTime, runtime.Version())
	default:
		fmt.Printf("Unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
}
