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

func (c *Producer) Publish(ctx context.Context, exchangeName, routingKey, payload string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type:       protocol.PublishRequest,
		Exchange:   exchangeName,
		RoutingKey: routingKey,
		Payload: model.Message{
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

func (c *Producer) Metrics(ctx context.Context) (*model.BrokerMetrics, error) {
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

func (c *Producer) QueueMetrics(ctx context.Context, queueName string) (*model.QueueMetric, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var req protocol.Request
	req = protocol.Request{
		Type:  protocol.MetricsRequest,
		Queue: queueName,
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

	for i := range resp.Metrics.Queues {
		if resp.Metrics.Queues[i].Name == queueName {
			return &resp.Metrics.Queues[i], nil
		}
	}

	return nil, errors.New("queue metrics not found for " + queueName)
}

func (c *Producer) CreateQueue(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type:  protocol.CreateQueueRequest,
		Queue: name,
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

func (c *Producer) DeleteQueue(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type:  protocol.DeleteQueueRequest,
		Queue: name,
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

func (c *Producer) ListQueues(ctx context.Context) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	req := protocol.Request{
		Type: protocol.ListQueuesRequest,
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

	queues := make([]string, 0, len(resp.Queues))
	for _, queue := range resp.Queues {
		queues = append(queues, queue.Name)
	}
	return queues, nil
}

func (c *Producer) BindQueue(ctx context.Context, exchangeName, queueName, bindingKey string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type:       protocol.BindQueueRequest,
		Exchange:   exchangeName,
		Queue:      queueName,
		BindingKey: bindingKey,
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

func (c *Producer) UnbindQueue(ctx context.Context, exchangeName, queueName, bindingKey string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type:       protocol.UnbindQueueRequest,
		Exchange:   exchangeName,
		Queue:      queueName,
		BindingKey: bindingKey,
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

func (c *Producer) CreateExchange(ctx context.Context, exchangeName string, exchangeType string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type:         protocol.CreateExchangeRequest,
		Exchange:     exchangeName,
		ExchangeType: exchangeType,
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

func (c *Producer) DeleteExchange(ctx context.Context, exchangeName string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	req := protocol.Request{
		Type:     protocol.DeleteExchangeRequest,
		Exchange: exchangeName,
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

func (c *Producer) ListExchanges(ctx context.Context) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	req := protocol.Request{
		Type: protocol.ListExchangesRequest,
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

	exchanges := make([]string, 0, len(resp.Exchanges))
	for _, exchange := range resp.Exchanges {
		exchanges = append(exchanges, exchange)
	}
	return exchanges, nil
}
