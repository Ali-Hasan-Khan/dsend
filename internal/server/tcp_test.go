package server

import (
	"context"
	"net"
	"testing"
	"time"
)

type mockLogger struct{}

func (mockLogger) Infof(string, ...any)  {}
func (mockLogger) Warnf(string, ...any)  {}
func (mockLogger) Errorf(string, ...any) {}

func TestShutdownClosesIdleConnections(t *testing.T) {
	// Reserve a port, then hand the address to the server.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv := New(addr, &mockBroker{}, mockLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Start(ctx)
	}()

	// Wait until the server accepts connections.
	var conn net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never accepted connections: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer conn.Close()

	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down within 2s — idle connection kept it alive")
	}

	// The client should observe the forced close.
	conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected read error on client connection after shutdown")
	}
}

func TestShutdownWhileAccepting(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv := New(addr, &mockBroker{}, mockLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Start(ctx)
	}()

	// Dial repeatedly while cancelling, stressing the Accept/trackConn race.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err == nil {
				c.Close()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down within 2s")
	}
	close(stop)
}
