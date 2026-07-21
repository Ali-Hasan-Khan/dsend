package client

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
)

type Producer struct {
	*baseClient
}

func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Producer{
		baseClient: newBaseClient(conn),
	}, nil
}

func (c *Producer) do(req protocol.Request) (*protocol.Response, error) {
	if err := c.encoder.Encode(&req); err != nil {
		return nil, err
	}

	_ = c.conn.SetReadDeadline(time.Now().Add(c.timeout))

	var resp protocol.Response
	if err := c.decoder.Decode(&resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *Producer) Publish(ctx context.Context, payload string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type: protocol.PublishRequest,
		Message: model.Message{
			Payload: payload,
		},
	}

	resp, err := c.do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return err
		}
	}

	if !resp.Success {
		return errors.New(resp.Error)
	}
	return nil
}

func (c *Producer) Metrics(ctx context.Context) (*model.Metric, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var req protocol.Request
	req = protocol.Request{
		Type: protocol.MetricsRequest,
	}

	resp, err := c.do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, err
		}
	}

	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	return &resp.Metrics, nil
}
