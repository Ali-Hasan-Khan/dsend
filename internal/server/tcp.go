package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
)

type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type Server struct {
	listenAddr string
	broker     engine.Broker
	logger     Logger
}

func New(listenAddr string, broker engine.Broker, log Logger) *Server {
	return &Server{
		listenAddr: listenAddr,
		broker:     broker,
		logger:     log,
	}
}

func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("Failed to start server: %v", err)
	}

	s.logger.Infof("TCP server running on port %v....", s.listenAddr)

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		s.logger.Infof("Shutting down TCP server gracefully...")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break // Exit the loop safely
			}
			s.logger.Warnf("Failed to accept connection: %v", err)
			continue
		}

		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			s.handleConnection(conn, s.broker)
		}(conn)
	}

	s.logger.Infof("Waiting for All clients to finish...")
	wg.Wait()
	s.logger.Infof("Server stopped safely.")
	return nil
}
