package client

import (
	"encoding/json"
	"net"
	"time"

	"github.com/google/uuid"
)

type baseClient struct {
	id      string
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	timeout time.Duration
}

func newBaseClient(conn net.Conn) *baseClient {
	return &baseClient{
		id:      uuid.NewString(),
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
		timeout: 5 * time.Second,
	}
}

func (c *baseClient) Close() error {
	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}
