package server

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

func (s *Server) handleConnection(conn net.Conn, b engine.Broker) {
	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()
	log.Printf("New client connected from: %s", clientAddr)

	var mu sync.Mutex
	var stopSubscribe chan struct{}
	var subwg sync.WaitGroup

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	defer func() {
		if stopSubscribe != nil {
			close(stopSubscribe)
			subwg.Wait()
		}
	}()
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
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: err == nil,
			})
			mu.Unlock()
		case protocol.AckRequest:
			err := b.Ack(req.AckToken)
			if err != nil {
				mu.Lock()
				encoder.Encode(protocol.Response{
					Success: false,
					Error:   err.Error(),
				})
				mu.Unlock()
				continue
			}
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: true,
			})
			mu.Unlock()
		case protocol.MetricsRequest:
			metrics := b.Metrics()
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: true,
				Metrics: metrics,
			})
			mu.Unlock()
		case protocol.SubscribeRequest:
			if stopSubscribe != nil {
				mu.Lock()
				encoder.Encode(protocol.Response{Success: false, Error: "Already subscribed"})
				mu.Unlock()
				continue
			}

			sess := session.NewConsumerSession(req.ID)
			b.Subscribe(sess)

			stopSubscribe = make(chan struct{})
			subwg.Add(1)
			go func(currentStop chan struct{}, currentSess *session.ConsumerSession) {
				defer subwg.Done()
				for {
					select {
					case delivery := <-currentSess.Deliveries:
						mu.Lock()
						encoder.Encode(protocol.Response{
							Success:  true,
							Message:  delivery.Message,
							AckToken: delivery.AckToken,
						})
						mu.Unlock()
					case <-currentStop:
						b.Unsubscribe(req.ID)
						return
					}
				}
			}(stopSubscribe, sess)
		case protocol.UnsubscribeRequest:
			close(stopSubscribe)
			subwg.Wait()

			stopSubscribe = nil

			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: true,
			})
			mu.Unlock()
		default:
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: false,
				Error:   "unknown request",
			})
			mu.Unlock()
		}
	}
}
