package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/broker"
)

type Server struct {
	listenAddr string
	broker     broker.Broker
}

func New(listenAddr string, broker broker.Broker) *Server {
	return &Server{
		listenAddr: listenAddr,
		broker:     broker,
	}
}

func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("Failed to start server: %v", err)
	}
	// defer listener.Close()

	log.Printf("TCP server running on port %v....", s.listenAddr[1:])

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		log.Println("Shutting down TCP server gracefully...")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break // Exit the loop safely
			}
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			s.handleConnection(conn, s.broker)
		}(conn)
	}

	log.Println("Waiting for All clients to finish...")
	wg.Wait()
	log.Println("Server stopped safely.")
	return nil
}
