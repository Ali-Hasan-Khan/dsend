package client

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
)

type Client struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

func NewClient(conn net.Conn) *Client {
	return &Client{
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

	return &protocol.Response{
		Success:  true,
		Error:    "",
		Message:  resp.Message,
		AckToken: resp.AckToken,
		Metrics:  resp.Metrics,
	}
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

func (c *Client) Consume() error {
	var req protocol.Request
	req = protocol.Request{
		Type: protocol.ConsumeRequest,
	}

	resp := c.do(req)

	if resp.Success {
		fmt.Printf("[Server Response] Success! Ack Token: %v\n", resp.AckToken)
	} else {
		fmt.Printf("[Server Response] Failed! Cause: %s\n", resp.Error)
	}

	return nil
}

func (c *Client) Ack(token string) error {
	var req protocol.Request
	req = protocol.Request{
		Type:     protocol.AckRequest,
		AckToken: token,
	}

	resp := c.do(req)

	if resp.Success {
		fmt.Println("[Server Response] Success! ")
	} else {
		fmt.Printf("[Server Response] Failed! Cause: %s\n", resp.Error)
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

func (c *Client) Subscribe(ID string) error {
	var req protocol.Request
	req = protocol.Request{
		Type: protocol.SubscribeRequest,
		ID:   ID,
	}

	if err := c.encoder.Encode(&req); err != nil {
		return fmt.Errorf("Fatal: Failed to marshal or send JSON payload: %v", err)
	}

	var resp protocol.Response
	if err := c.decoder.Decode(&resp); err != nil {
		return fmt.Errorf("Fatal: Failed to receive or parse server response stream: %v", err)
	}

	if resp.Success {
		fmt.Printf("[Server Response] Success! Ack Token: %v\n", resp.AckToken)
	} else {
		fmt.Printf("[Server Response] Failed! Cause: %s\n", resp.Error)
	}

	return nil
}
