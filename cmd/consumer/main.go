package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/Ali-Hasan-Khan/dsend/internal/client"
)

func main() {

	serverAddr := "localhost:8080"
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := client.NewClient(conn)

	fmt.Printf("Connected Consumer ID: %s to %s:\n", client.Id, serverAddr)
	// fmt.Println("1. consume\n2. ack <ack_token>")
	fmt.Println("Type 'exit' to quit.")
	fmt.Print("> ")

	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe()
	go client.Decoder(ctx)

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		msg := strings.TrimSpace(scanner.Text())

		if len(strings.Split(msg, " ")) > 1 {
			token := strings.Split(msg, " ")[1]
			client.Ack(token)
			fmt.Print("> ")
			continue
		}

		if msg == "unsubscribe" {
			client.Unsubscribe()
			fmt.Println("Unsubscribed successfully!")
			fmt.Print("> ")
			continue
		}

		if msg == "" {
			fmt.Print("> ")
			continue
		}

		if msg == "exit" {
			cancel()
			fmt.Println("Closing connection. Goodbye!")
			return
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Terminal input scanner error: %v", err)
	}

}
