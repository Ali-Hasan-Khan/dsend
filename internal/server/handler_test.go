package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
)

type mockBroker struct {
	engine.Broker
	metricsCalls atomic.Int32
}

func (b *mockBroker) Metrics() model.BrokerMetrics {
	b.metricsCalls.Add(1)
	return model.BrokerMetrics{
		Total: model.Metric{ProducedCount: 7},
	}
}

func serveHandler(t *testing.T, broker engine.Broker) net.Conn {
	t.Helper()
	serverConn, clientConn := net.Pipe()

	srv := New("", broker, mockLogger{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go srv.handleConnection(ctx, serverConn, broker)
	t.Cleanup(func() {
		serverConn.Close()
		clientConn.Close()
	})
	return clientConn
}

func TestHandlerRejectUnknownVersions(t *testing.T) {
	tests := []struct {
		name    string
		version int
	}{
		{"future version", 99},
		{"negative version", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			broker := &mockBroker{}
			client := serveHandler(t, broker)
			client.SetReadDeadline(time.Now().Add(time.Second))

			if err := json.NewEncoder(client).Encode(protocol.Request{
				Version: tc.version,
				Type:    protocol.MetricsRequest,
			}); err != nil {
				t.Fatal(err)
			}

			var resp protocol.Response
			if err := json.NewDecoder(client).Decode(&resp); err != nil {
				t.Fatal(err)
			}

			if resp.Success {
				t.Fatal("expected rejection, got success")
			}

			if resp.Error != ErrUnsupportedVersion.Error() {
				t.Fatalf("error = %q, want %q", resp.Error, ErrUnsupportedVersion.Error())
			}

			if got := broker.metricsCalls.Load(); got != 0 {
				t.Fatalf("broker.Metrics called %d times, want 0", got)
			}

			buf := make([]byte, 1)
			if _, err := client.Read(buf); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("expected connection close, got %v", err)
			}
		})
	}
}

func TestHandlerAcceptsKnownVersions(t *testing.T) {
	tests := []struct {
		name    string
		version int
	}{
		{"current version", protocol.CurrentVersion},
		{"legacy client (field omitted)", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			broker := &mockBroker{}
			client := serveHandler(t, broker)
			client.SetReadDeadline(time.Now().Add(2 * time.Second))

			req := protocol.Request{Type: protocol.MetricsRequest, Version: tc.version}
			if err := json.NewEncoder(client).Encode(req); err != nil {
				t.Fatal(err)
			}

			var resp protocol.Response
			if err := json.NewDecoder(client).Decode(&resp); err != nil {
				t.Fatal(err)
			}

			if !resp.Success {
				t.Fatalf("expected success, got error: %q", resp.Error)
			}
			if resp.Metrics.Total.ProducedCount != 7 {
				t.Fatalf("ProducedCount = %d, want 7", resp.Metrics.Total.ProducedCount)
			}

			if got := broker.metricsCalls.Load(); got != 1 {
				t.Fatalf("broker.Metrics called %d times, want 1", got)
			}
		})
	}
}
