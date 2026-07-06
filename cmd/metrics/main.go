package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/client"
)

func main() {

	serverAddr := "localhost:8080"
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	fmt.Printf("Connected Metrics client to %s.\n", serverAddr)

	client := client.NewClient(conn)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			client.Metrics()
		case <-sigChan:
			return
		}
	}

}
