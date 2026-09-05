package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/client"
	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

// e2eHarness spins up a real broker + TCP server backed by a WAL file.
// Restarting with the same walPath simulates a broker crash: the WAL is
// closed cleanly, then a fresh broker replays it on boot.
type e2eHarness struct {
	t       *testing.T
	addr    string
	wal     storage.WAL
	broker  engine.Broker
	srv     *Server
	cancel  context.CancelFunc
	stopped chan error
}

func reservePort(t *testing.T) string {
	t.Helper()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitDialable(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server at %s never accepted connections: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newE2EHarness(t *testing.T, walPath string) *e2eHarness {
	t.Helper()

	wal, err := storage.NewFileWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	broker, err := engine.NewBroker(engine.DefaultConfig(), wal, mockLogger{})
	if err != nil {
		t.Fatal(err)
	}

	addr := reservePort(t)
	srv := New(addr, broker, mockLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	broker.Start(ctx)
	stopped := make(chan error, 1)
	go func() {
		stopped <- srv.Start(ctx)
	}()
	waitDialable(t, addr)

	return &e2eHarness{
		t: t, addr: addr, wal: wal, broker: broker,
		srv: srv, cancel: cancel, stopped: stopped,
	}
}

func (h *e2eHarness) shutdown() {
	h.t.Helper()

	h.cancel()
	select {
	case err := <-h.stopped:
		if err != nil {
			h.t.Fatalf("server did not stop cleanly: %v", err)
		}
	case <-time.After(5 * time.Second):
		h.t.Fatal("server did not stop within 5s")
	}
	h.broker.Shutdown()
	if err := h.wal.Close(); err != nil {
		h.t.Fatalf("wal close: %v", err)
	}
}

func (h *e2eHarness) producer(t *testing.T) *client.Producer {
	t.Helper()

	p, err := client.NewProducer(h.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func (h *e2eHarness) consumer(t *testing.T) *client.Consumer {
	t.Helper()

	c, err := client.NewConsumer(h.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func mustNoErr(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func receiveOne(t *testing.T, c *client.Consumer) *client.ReceivedMessage {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return msg
}

// waitForCondition polls cond until true or the timeout elapses. Client Ack
// is fire-and-forget, so broker state observed over a separate connection
// may lag the client's last call.
func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestE2EPublishSubscribeAck(t *testing.T) {
	h := newE2EHarness(t, filepath.Join(t.TempDir(), "wal.log"))
	defer h.shutdown()

	ctx := context.Background()
	p := h.producer(t)

	mustNoErr(t, p.CreateQueue(ctx, "orders"))
	mustNoErr(t, p.BindQueue(ctx, "default", "orders", "orders"))
	mustNoErr(t, p.Publish(ctx, "default", "orders", "hello", 0))

	c := h.consumer(t)
	mustNoErr(t, c.Subscribe("orders"))

	msg := receiveOne(t, c)
	if msg.Payload != "hello" {
		t.Fatalf("payload = %q, want hello", msg.Payload)
	}
	mustNoErr(t, c.Ack(msg.AckToken))

	waitForCondition(t, "ack to register", func() bool {
		m, err := p.QueueMetrics(ctx, "orders")
		if err != nil {
			return false
		}
		return m.ProducedCount == 1 && m.AckedCount == 1 && m.QueueDepth == 0
	})
}

func TestE2ERestartRecoversUnacked(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")

	h := newE2EHarness(t, walPath)
	ctx := context.Background()
	p := h.producer(t)

	mustNoErr(t, p.CreateQueue(ctx, "jobs"))
	mustNoErr(t, p.BindQueue(ctx, "default", "jobs", "jobs"))
	mustNoErr(t, p.Publish(ctx, "default", "jobs", "m1", 0))
	mustNoErr(t, p.Publish(ctx, "default", "jobs", "m2", 0))

	c := h.consumer(t)
	mustNoErr(t, c.Subscribe("jobs"))
	if got := receiveOne(t, c).Payload; got != "m1" {
		t.Fatalf("first payload = %q, want m1", got)
	}
	// m1 deliberately left unacked: after a restart it must come back.
	h.shutdown()

	h2 := newE2EHarness(t, walPath)
	defer h2.shutdown()

	c2 := h2.consumer(t)
	mustNoErr(t, c2.Subscribe("jobs"))

	if got := receiveOne(t, c2).Payload; got != "m1" {
		t.Fatalf("recovered payload = %q, want m1 (unacked redelivery)", got)
	}
	second := receiveOne(t, c2)
	mustNoErr(t, c2.Ack(second.AckToken))

	p2 := h2.producer(t)
	waitForCondition(t, "ack to register", func() bool {
		m, err := p2.QueueMetrics(ctx, "jobs")
		if err != nil {
			return false
		}
		return m.QueueDepth == 0 && m.InflightCount == 1
	})
}

func rawDial(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func assertGarbageClosesConn(t *testing.T, addr string) {
	t.Helper()

	conn := rawDial(t, addr)
	if _, err := fmt.Fprint(conn, "this is not json\n"); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the server to close the connection on malformed input")
	}
}

func assertDoubleSubscribeErrors(t *testing.T, h *e2eHarness) {
	t.Helper()

	c := h.consumer(t)
	mustNoErr(t, c.Subscribe("default"))
	mustNoErr(t, c.Subscribe("default"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.Receive(ctx)
	if err == nil || !strings.Contains(err.Error(), "Already subscribed") {
		t.Fatalf("expected already-subscribed error, got: %v", err)
	}
}

func assertBogusAckRejected(t *testing.T, addr string) {
	t.Helper()

	conn := rawDial(t, addr)
	req := protocol.Request{Type: protocol.AckRequest, AckToken: "bogus", Version: protocol.CurrentVersion}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		t.Fatal(err)
	}

	var resp protocol.Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatal("expected rejection for bogus ack token")
	}
}

func TestE2EProtocolErrors(t *testing.T) {
	h := newE2EHarness(t, filepath.Join(t.TempDir(), "wal.log"))
	defer h.shutdown()

	t.Run("garbage closes connection", func(t *testing.T) {
		assertGarbageClosesConn(t, h.addr)
	})
	t.Run("double subscribe errors", func(t *testing.T) {
		assertDoubleSubscribeErrors(t, h)
	})
	t.Run("bogus ack rejected", func(t *testing.T) {
		assertBogusAckRejected(t, h.addr)
	})
}
