package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"

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

func encode(mu *sync.Mutex, conn net.Conn, encoder *json.Encoder, resp *protocol.Response) error {
	mu.Lock()
	defer mu.Unlock()

	if err := conn.SetWriteDeadline(time.Now().Add(100 * time.Second)); err != nil {
		return err
	}
	return encoder.Encode(resp)
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn, b engine.Broker) {
	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()
	s.logger.Infof("New client connected from: %s", clientAddr)

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
		select {
		case <-ctx.Done():
			return
		default:
		}

		var req protocol.Request
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				s.logger.Infof("Client disconnected: %v", err)
				return
			}
			s.logger.Errorf("Error decoding JSON from %s: %v", clientAddr, err)
			return
		}

		switch req.Type {
		case protocol.PublishRequest:
			err := b.Publish(req.Exchange, req.RoutingKey, req.Payload, req.TTL)
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.AckRequest:
			err := b.Ack(req.AckToken)
			if err != nil {
				if err := encode(&mu, conn, encoder, &protocol.Response{
					Success: false,
					Error:   errorString(err),
				}); err != nil {
					s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
					return
				}
				continue
			}
		case protocol.MetricsRequest:
			if req.Queue != "" {
				metric, err := b.QueueMetrics(req.Queue)

				if err := encode(&mu, conn, encoder, &protocol.Response{
					Success: err == nil,
					Error:   errorString(err),
					Metrics: model.BrokerMetrics{
						Queues: []model.QueueMetric{{
							Name:   req.Queue,
							Metric: metric,
						}},
						Total: metric,
					},
				}); err != nil {
					s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
					return
				}
				continue
			}
			metrics := b.Metrics()
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: true,
				Metrics: metrics,
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.SubscribeRequest:
			if stopSubscribe != nil {
				if err := encode(&mu, conn, encoder, &protocol.Response{
					Success: false,
					Error:   "Already subscribed",
				}); err != nil {
					s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
					return
				}
				continue
			}

			sessionID := req.ID
			queueName := req.Queue

			sess := session.NewConsumerSession(sessionID)
			if err := b.Subscribe(queueName, sess); err != nil {
				if err := encode(&mu, conn, encoder, &protocol.Response{
					Success: false,
					Error:   errorString(err),
				}); err != nil {
					s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
					return
				}
				continue
			}

			stopSubscribe = make(chan struct{})
			subwg.Add(1)
			go func(currentStop chan struct{}, currentSess *session.ConsumerSession, sessionID, queueName string) {
				defer subwg.Done()
				for {
					select {
					case delivery := <-currentSess.Deliveries:
						if err := encode(&mu, conn, encoder, &protocol.Response{
							Success:  true,
							Message:  delivery.Message,
							AckToken: delivery.AckToken,
						}); err != nil {
							s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
							b.Unsubscribe(queueName, sessionID)
							return
						}
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
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: true,
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.CreateQueueRequest:
			err := b.CreateQueue(req.Queue)

			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.DeleteQueueRequest:
			err := b.DeleteQueue(req.Queue)
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.ListQueuesRequest:
			names := b.ListQueues()
			queues := make([]model.QueueMetric, 0, len(names))
			for _, name := range names {
				queues = append(queues, model.QueueMetric{Name: name})
			}
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: true,
				Queues:  queues,
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.BindQueueRequest:
			err := b.BindQueue(req.Exchange, req.Queue, req.BindingKey)
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.UnbindQueueRequest:
			err := b.UnbindQueue(req.Exchange, req.Queue, req.BindingKey)
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.CreateExchangeRequest:
			err := b.CreateExchange(req.Exchange, req.ExchangeType)
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.DeleteExchangeRequest:
			err := b.DeleteExchange(req.Exchange)
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: err == nil,
				Error:   errorString(err),
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		case protocol.ListExchangesRequest:
			exchanges := b.ListExchanges()
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success:   true,
				Exchanges: exchanges,
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		default:
			if err := encode(&mu, conn, encoder, &protocol.Response{
				Success: false,
				Error:   "unknown request",
			}); err != nil {
				s.logger.Errorf("Error encoding JSON into %s: %v", clientAddr, err)
				return
			}
		}
	}
}
