package client

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
)

type Consumer struct {
	*baseClient
}

func NewConsumer(addr string) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		baseClient: newBaseClient(conn),
	}, nil
}

func (c *Consumer) Receive(ctx context.Context) (*ReceivedMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c.conn.Close()
		case <-watchDone:
		}
	}()

	var resp protocol.Response
	if err := c.decoder.Decode(&resp); err != nil {
		close(watchDone)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, err
		}
	}

	close(watchDone)

	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	r := &ReceivedMessage{
		ID:       resp.Message.ID,
		Payload:  resp.Message.Payload,
		AckToken: resp.AckToken,
	}

	return r, nil
}

func (c *Consumer) Ack(token string) error {
	var req protocol.Request
	req = protocol.Request{
		Type:     protocol.AckRequest,
		AckToken: token,
	}

	if err := c.encoder.Encode(&req); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) Subscribe() error {
	var req protocol.Request
	req = protocol.Request{
		Type: protocol.SubscribeRequest,
		ID:   c.id,
	}

	if err := c.encoder.Encode(&req); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) Unsubscribe() error {
	var req protocol.Request
	req = protocol.Request{
		Type: protocol.UnsubscribeRequest,
	}

	if err := c.encoder.Encode(&req); err != nil {
		return err
	}

	return nil
}
