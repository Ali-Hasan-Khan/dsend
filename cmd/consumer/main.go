package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/Ali-Hasan-Khan/dsend/internal/client"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
)

func main() {

	serverAddr := "localhost:8080"
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	fmt.Printf("Connected Consumer to %s. Type your commands below in the following format:\n", serverAddr)
	fmt.Println("1. consume\n2. ack <ack_token>")
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

		msgs := strings.Split(msg, " ")
		msgType := msgs[0]

		switch msgType {
		case protocol.ConsumeRequest:
			if err := client.Consume(); err != nil {
				log.Fatal(err)
			}
		case protocol.AckRequest:
			if len(msgs) != 2 {
				fmt.Print("Usage: ack <token>\n> ")
				continue
			}
			msgToken := msgs[1]
			if err := client.Ack(msgToken); err != nil {
				log.Fatal(err)
			}
		default:
			fmt.Print("Invalid type(only consume,ack supported)!\n> ")
			continue
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Terminal input scanner error: %v", err)
	}

}
