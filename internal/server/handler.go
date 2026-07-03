package server

import (
	"encoding/json"
	"io"
	"log"
	"net"

	"github.com/Ali-Hasan-Khan/dsend/internal/broker"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

func (s *Server) handleConnection(conn net.Conn, b broker.Broker) {
	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()
	log.Printf("New client connected from: %s", clientAddr)

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	for {
		var req protocol.Request
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				log.Printf("Client disconnected: %v", err)
				return
			}
			log.Printf("Error decoding JSON from %s: %v", clientAddr, err)
			return
		}

		log.Printf("[%s] Received request type: %s", clientAddr, req.Type)

		switch req.Type {
		case protocol.PublishRequest:
			err := b.Publish(req.Message)
			encoder.Encode(protocol.Response{
				Success: err == nil,
			})
		case protocol.ConsumeRequest:
			delivery, ok := b.Consume()
			if !ok {
				encoder.Encode(protocol.Response{
					Success: false,
					Error:   "Broker closed",
				})
				continue
			}
			encoder.Encode(protocol.Response{
				Success:  true,
				Message:  delivery.Message,
				AckToken: delivery.AckToken,
			})
		case protocol.AckRequest:
			err := b.Ack(req.AckToken)
			if err != nil {
				encoder.Encode(protocol.Response{
					Success: false,
					Error:   err.Error(),
				})
				continue
			}
			encoder.Encode(protocol.Response{
				Success: true,
			})
		case protocol.MetricsRequest:
			metrics := b.Metrics()
			encoder.Encode(protocol.Response{
				Success: true,
				Metrics: metrics,
			})
		case protocol.SubscribeRequest:
			session := session.NewConsumerSession(req.ID)
			b.Subscribe(session)
			for {
				delivery := <-session.Deliveries
				encoder.Encode(protocol.Response{
					Success:  true,
					Message:  delivery.Message,
					AckToken: delivery.AckToken,
				})
			}
		default:
			encoder.Encode(protocol.Response{
				Success: false,
				Error:   "unknown request",
			})
		}
	}
}
