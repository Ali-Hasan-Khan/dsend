package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
	"github.com/google/uuid"
)

type Client struct {
	Id      string
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		Id:      uuid.NewString(),
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
	}
}

func (c *Client) do(req protocol.Request) *protocol.Response {
	if err := c.encoder.Encode(&req); err != nil {
		return &protocol.Response{
			Success: false,
			Error:   fmt.Sprintf("Fatal: Failed to marshal or send JSON payload: %v", err),
		}
	}

	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var resp protocol.Response
	if err := c.decoder.Decode(&resp); err != nil {
		return &protocol.Response{
			Success: false,
			Error:   fmt.Sprintf("Fatal: Failed to receive or parse server response stream: %v", err),
		}
	}

	return &resp
}

func (c *Client) Publish(msg string) error {
	req := protocol.Request{
		Type: protocol.PublishRequest,
		Message: model.Message{
			Payload: msg,
		},
	}

	resp := c.do(req)

	if resp.Success {
		fmt.Println("[Server Response] Success!")
	} else {
		fmt.Printf("[Server Response] Failed! Cause: %s\n", resp.Error)
	}
	return nil
}

func (c *Client) Decoder(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Closing Decoder!")
			return ctx.Err()
		default:
		}

		var resp protocol.Response
		if err := c.decoder.Decode(&resp); err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				fmt.Printf("Fatal: Failed to receive or parse server response stream: %v\n> ", err)
				return err
			}
		}

		if resp.Success {
			if resp.AckToken == "" {
				fmt.Print("[Server Response] Success!\n> ")
			} else {
				fmt.Printf("[Server Response] Success! Ack Token: %v\n> ", resp.AckToken)
			}
		} else {
			fmt.Printf("[Server Response] Failed! Cause: %s\n> ", resp.Error)
		}

	}

}

func (c *Client) Ack(token string) error {
	var req protocol.Request
	req = protocol.Request{
		Type:     protocol.AckRequest,
		AckToken: token,
	}

	if err := c.encoder.Encode(&req); err != nil {
		return fmt.Errorf("Fatal: Failed to marshal or send JSON payload: %v", err)
	}

	return nil
}

func (c *Client) Metrics() error {
	var req protocol.Request
	req = protocol.Request{
		Type: protocol.MetricsRequest,
	}

	resp := c.do(req)

	if resp.Success {
		fmt.Println("[Server Response] Success! ")
	} else {
		fmt.Printf("[Server Response] Failed! Cause: %s\n", resp.Error)
	}

	return nil
}

func (c *Client) Subscribe() error {
	var req protocol.Request
	req = protocol.Request{
		Type: protocol.SubscribeRequest,
		ID:   c.Id,
	}

	if err := c.encoder.Encode(&req); err != nil {
		return fmt.Errorf("Fatal: Failed to marshal or send JSON payload: %v", err)
	}

	return nil
}

func (c *Client) Unsubscribe() error {
	var req protocol.Request
	req = protocol.Request{
		Type: protocol.UnsubscribeRequest,
	}

	if err := c.encoder.Encode(&req); err != nil {
		return fmt.Errorf("Fatal: Failed to marshal or send JSON payload: %v", err)
	}

	return nil
}
