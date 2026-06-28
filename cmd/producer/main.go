package main

import (
	"bufio"
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
	fmt.Printf("Connected Producer to %s. Type your messages below.\n", serverAddr)
	fmt.Println("Type 'exit' to quit.")
	fmt.Print("> ")

	client := client.NewClient(conn)

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {

		msg := scanner.Text()
		msg = strings.TrimSpace(msg)

		if msg == "exit" {
			fmt.Println("Closing connection. Goodbye!")
			return
		}

		if msg == "" {
			continue
		}

		if err := client.Publish(msg); err != nil {
			log.Fatal(err)
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Terminal input scanner error: %v", err)
	}

}
