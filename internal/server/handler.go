package server

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"

	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/protocol"
	"github.com/Ali-Hasan-Khan/dsend/internal/session"
)

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

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
			err := b.Publish(req.Queue, req.Message)
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			})
			mu.Unlock()
		case protocol.AckRequest:
			err := b.Ack(req.AckToken)
			if err != nil {
				mu.Lock()
				encoder.Encode(protocol.Response{
					Success: false,
					Error:   errorString(err),
				})
				mu.Unlock()
				continue
			}
		case protocol.MetricsRequest:
			if req.Queue != "" {
				metric, err := b.QueueMetrics(req.Queue)

				mu.Lock()
				_ = encoder.Encode(protocol.Response{
					Success: err == nil,
					Error:   errorString(err),
					Metrics: model.BrokerMetrics{
						Queues: []model.QueueMetric{{
							Name:   req.Queue,
							Metric: metric,
						}},
						Total: metric,
					},
				})
				mu.Unlock()
				continue
			}
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

			sessionID := req.ID
			queueName := req.Queue

			sess := session.NewConsumerSession(sessionID)
			if err := b.Subscribe(queueName, sess); err != nil {
				mu.Lock()
				encoder.Encode(protocol.Response{Success: false, Error: errorString(err)})
				mu.Unlock()
				continue
			}

			stopSubscribe = make(chan struct{})
			subwg.Add(1)
			go func(currentStop chan struct{}, currentSess *session.ConsumerSession, sessionID, queueName string) {
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
						b.Unsubscribe(queueName, sessionID)
						return
					}
				}
			}(stopSubscribe, sess, sessionID, queueName)
		case protocol.UnsubscribeRequest:
			if stopSubscribe != nil {
				close(stopSubscribe)
				subwg.Wait()
			}

			stopSubscribe = nil

			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: true,
			})
			mu.Unlock()
		case protocol.CreateQueueRequest:
			err := b.CreateQueue(req.Queue)
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			})
			mu.Unlock()
		case protocol.DeleteQueueRequest:
			err := b.DeleteQueue(req.Queue)
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			})
			mu.Unlock()
		case protocol.ListQueuesRequest:
			names := b.ListQueues()
			queues := make([]model.QueueMetric, 0, len(names))
			for _, name := range names {
				queues = append(queues, model.QueueMetric{Name: name})
			}
			mu.Lock()
			encoder.Encode(protocol.Response{
				Success: true,
				Queues:  queues,
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
