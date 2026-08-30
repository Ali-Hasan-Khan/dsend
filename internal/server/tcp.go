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

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

func New(listenAddr string, broker engine.Broker, log Logger) *Server {
	return &Server{
		listenAddr: listenAddr,
		broker:     broker,
		logger:     log,
		conns:      make(map[net.Conn]struct{}),
	}
}

func (s *Server) trackConn(conn net.Conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	s.conns[conn] = struct{}{}
}

func (s *Server) untrackConn(conn net.Conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	delete(s.conns, conn)
}

func (s *Server) closeConns() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for conn := range s.conns {
		conn.Close()
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
		s.closeConns()
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

		s.trackConn(conn)

		select {
		case <-ctx.Done():
			s.untrackConn(conn)
			conn.Close()
			continue
		default:
		}

		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer s.untrackConn(conn)
			s.handleConnection(ctx, conn, s.broker)
		}(conn)
	}

	s.logger.Infof("Waiting for All clients to finish...")
	wg.Wait()
	s.logger.Infof("Server stopped safely.")
	return nil
}
