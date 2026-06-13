package main

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"
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
}

type RemoveMessage struct {
	idx string
	msg Message
}

func NewQueue(capacity int) *Queue {
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
	queue := NewQueue(queueCapacity)

	deliveredMessages := make(map[string]int)
	sum := 0
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
				queue.Push(message)
			}
		}(i)
	}

	wgGen.Add(1)
	go func() {
		defer wgGen.Done()
		for {
			pLock.Lock()
			currentSum := sum
			pLock.Unlock()

			queue.mu.Lock()
			currentInflight := len(queue.inFlight)
			isClosed := queue.closed
			queue.mu.Unlock()

			fmt.Printf("Acked count: %d\033[K\n", currentSum)
			fmt.Printf("Inflight:    %d\033[K\n", currentInflight)

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
				value, ok := queue.Pop()
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

				queue.Ack(value.ID)

				pLock.Lock()
				ackMessages[value.ID]++
				sum++
				pLock.Unlock()
			}
		}(i)
	}

	wgGen.Add(1)
	go func() {
		defer wgGen.Done()
		for !queue.IsClosed() {
			time.Sleep(time.Second * 2)
			queue.StartRedeliveryLoop()
		}
	}()

	wg1.Wait()
	for {

		if queue.IsDone() {
			break
		}

		time.Sleep(time.Second)
	}
	queue.Close()
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
	fmt.Println("Messages stored in DLQ:", len(queue.deadLetterQueue))
	fmt.Println("Messages duplicated:", duplicates)
}

func (q *Queue) IsDone() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	done := len(q.inFlight) == 0 && q.size == 0
	return done
}

func (q *Queue) StartRedeliveryLoop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	removeMessage := make([]RemoveMessage, 0)
	for idx, item := range q.inFlight {
		if time.Since(item.DeliveredAt) > time.Millisecond*time.Duration(timeout) {
			removeMessage = append(removeMessage, RemoveMessage{
				idx: idx,
				msg: item.Message,
			})
		}
	}

	for _, item := range removeMessage {
		if !q.closed && q.size == q.cap {
			continue // if broker full, skip
		}
		if item.msg.Retry >= maxRetries {
			q.deadLetterQueue = append(q.deadLetterQueue, item.msg)
			delete(q.inFlight, item.idx)
			continue
		}
		// repush
		msg := item.msg
		msg.Retry++
		q.enqueue(msg)
		q.condCons.Signal()
		// delete
		delete(q.inFlight, item.idx)
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
	q.condCons.Signal()
}

func (q *Queue) Pop() (Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.size == 0 {
		q.condCons.Wait()
	}
	if q.closed && q.size == 0 {
		return Message{}, false
	}
	message := q.readyQueue[q.head]
	q.dequeue()
	q.inFlight[message.ID] = InFlightMessage{
		message,
		time.Now(),
	}
	q.condProd.Signal()
	return message, true
}

func (q *Queue) Ack(messageID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.inFlight[messageID]; !ok {
		return // for now ignore invalid messageID
	}
	delete(q.inFlight, messageID)
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
