package main

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	nProducers      int = 50
	nConsumers      int = 50
	queueCapacity   int = 20
	nMsgPerProducer int = 100
	timeout         int = 1000 // in ms
	maxRetries      int = 3
)

type Message struct {
	ID        string
	Payload   []byte
	Timestamp time.Time
	Retry     int
}

type InFlightMessage struct {
	Message
	DeliveredAt time.Time
}

type Queue struct {
	readyQueue []Message // ready queue
	cap        int
	size       int
	head       int
	tail       int
	closed     bool
	mu         sync.Mutex

	condProd *sync.Cond
	condCons *sync.Cond

	inFlight        map[string]InFlightMessage // inflight hashmap
	deadLetterQueue []Message                  // dead letter queue

	ackedCount    int
	producedCount int
	dlqCount      int
}

type ExpiredDelivery struct {
	token   string
	message Message
}

type Delivery struct {
	Message
	AckToken string
}

type Metric struct {
	producedCount int
	ackedCount    int
	dlqCount      int
	inflightCount int
}

func NewBroker(capacity int) *Queue {
	q := &Queue{
		readyQueue: make([]Message, capacity),
		cap:        capacity,
		size:       0,
		head:       0,
		tail:       0,
		closed:     false,
		inFlight:   make(map[string]InFlightMessage),
	}

	q.condProd = sync.NewCond(&q.mu)
	q.condCons = sync.NewCond(&q.mu)
	return q
}

func main() {
	broker := NewBroker(queueCapacity)

	deliveredMessages := make(map[string]int)
	ackMessages := make(map[string]int)
	var pLock sync.Mutex

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	var wgGen sync.WaitGroup

	for i := 1; i <= nProducers; i++ {
		wg1.Add(1)
		go func(id int) {
			defer wg1.Done()
			for msg := 1; msg <= nMsgPerProducer; msg++ {
				currID := (id-1)*nMsgPerProducer + msg
				msgStr := fmt.Sprintf("producer-%d-msg-%d", id, msg)
				message := Message{
					ID:        strconv.Itoa(currID),
					Payload:   []byte(msgStr),
					Timestamp: time.Now(),
					Retry:     0,
				}
				broker.Push(message)
			}
		}(i)
	}

	wgGen.Add(1)
	go func() {
		defer wgGen.Done()
		for {
			metrics := broker.GetMetrics()

			broker.mu.Lock()
			isClosed := broker.closed
			broker.mu.Unlock()

			fmt.Printf("Acked count: %d\033[K\n", metrics.ackedCount)
			fmt.Printf("Inflight:    %d\033[K\n", metrics.inflightCount)

			// If the queue is finished, exit the print loop
			if isClosed {
				break
			}

			time.Sleep(100 * time.Millisecond)

			fmt.Print("\033[F\033[F")
		}
	}()

	for i := 1; i <= nConsumers; i++ {
		wg2.Add(1)
		go func(id int) {
			defer wg2.Done()
			for {
				value, ok := broker.Pop()
				if !ok {
					break
				}
				pLock.Lock()
				deliveredMessages[value.ID]++
				pLock.Unlock()

				// simulate consumer crash 20% of the time
				if rand.IntN(100) < 20 {
					continue
				}

				broker.Ack(value.AckToken)

				pLock.Lock()
				ackMessages[value.ID]++
				pLock.Unlock()
			}
		}(i)
	}

	wgGen.Add(1)
	go func() {
		defer wgGen.Done()
		for !broker.IsClosed() {
			time.Sleep(time.Second * 2)
			broker.StartRedeliveryLoop()
		}
	}()

	wg1.Wait()
	for {

		if broker.IsDone() {
			break
		}

		time.Sleep(time.Second)
	}
	broker.Close()
	wg2.Wait()
	wgGen.Wait()
	fmt.Println()

	// Metrics
	var totalDelivered = 0
	for _, c := range deliveredMessages {
		totalDelivered += c
	}
	var duplicates = 0
	var totalAcks = 0
	for _, c := range ackMessages {
		totalAcks += c
		if c > 1 {
			duplicates += (c - 1)
		}
	}
	fmt.Println("Messages delivered:", totalDelivered)
	fmt.Println("Messages consumed:", totalAcks)
	fmt.Println("Messages lost:", (nProducers*nMsgPerProducer)-len(ackMessages))
	fmt.Println("Messages stored in DLQ:", len(broker.deadLetterQueue))
	fmt.Println("Messages duplicated:", duplicates)
}

func (q *Queue) IsDone() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	done := q.ackedCount+q.dlqCount == q.producedCount
	return done
}

func (q *Queue) StartRedeliveryLoop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	removeMessage := make([]ExpiredDelivery, 0)
	for idx, item := range q.inFlight {
		if time.Since(item.DeliveredAt) > time.Millisecond*time.Duration(timeout) {
			removeMessage = append(removeMessage, ExpiredDelivery{
				token:   idx,
				message: item.Message,
			})
		}
	}

	for _, item := range removeMessage {
		if !q.closed && q.size == q.cap {
			continue // if broker full, skip
		}
		if item.message.Retry >= maxRetries {
			q.deadLetterQueue = append(q.deadLetterQueue, item.message)
			q.dlqCount++
			delete(q.inFlight, item.token)
			continue
		}
		// repush
		msg := item.message
		msg.Retry++
		q.enqueue(msg)
		q.condCons.Signal()
		// delete
		delete(q.inFlight, item.token)
	}

}

func (q *Queue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.condProd.Broadcast()
	q.condCons.Broadcast()
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

func (q *Queue) Push(message Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.size == q.cap {
		q.condProd.Wait()
	}
	if q.closed {
		return
	}
	q.enqueue(message)
	q.producedCount++
	q.condCons.Signal()
}

func (q *Queue) Pop() (Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.size == 0 {
		q.condCons.Wait()
	}
	if q.closed && q.size == 0 {
		return Delivery{}, false
	}
	message := q.readyQueue[q.head]
	token := uuid.NewString()
	q.dequeue()
	q.inFlight[token] = InFlightMessage{
		Message:     message,
		DeliveredAt: time.Now(),
	}
	q.condProd.Signal()
	delivery := Delivery{
		Message:  message,
		AckToken: token,
	}
	return delivery, true
}

func (q *Queue) Ack(token string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.inFlight[token]; !ok {
		return // for now ignore invalid messageID
	}
	delete(q.inFlight, token)
	q.ackedCount++
}

func (q *Queue) GetMetrics() Metric {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Metric{
		producedCount: q.producedCount,
		ackedCount:    q.ackedCount,
		dlqCount:      q.dlqCount,
		inflightCount: len(q.inFlight),
	}
}

func (q *Queue) enqueue(message Message) {
	q.readyQueue[q.tail] = message
	q.tail = (q.tail + 1) % q.cap
	q.size++
}

func (q *Queue) dequeue() {
	q.readyQueue[q.head] = Message{}
	q.head = (q.head + 1) % q.cap
	q.size--
}

func (q *Queue) String() string {
	return fmt.Sprint(q.readyQueue)
}
