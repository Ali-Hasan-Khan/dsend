package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	nProducers      int = 50
	nConsumers      int = 50
	queueCapacity   int = 10
	nMsgPerProducer int = 10000
)

type Message struct {
	ID        string
	Payload   []byte
	Timestamp time.Time
}

type Queue struct {
	messages []Message
	cap      int
	size     int
	head     int
	tail     int
	closed   bool
	mu       sync.Mutex

	condProd *sync.Cond
	condCons *sync.Cond
}

func NewQueue(capacity int) *Queue {
	q := &Queue{
		messages: make([]Message, capacity),
		cap:      capacity,
		size:     0,
		head:     0,
		tail:     0,
		closed:   false,
	}

	q.condProd = sync.NewCond(&q.mu)
	q.condCons = sync.NewCond(&q.mu)
	return q
}

func main() {
	queue := NewQueue(queueCapacity)

	processedMessages := make(map[string]int)
	var pLock sync.Mutex

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
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
				}
				queue.Push(message)
			}
		}(i)
	}

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
				processedMessages[value.ID] += 1
				pLock.Unlock()
			}
		}(i)
	}
	wg1.Wait()
	queue.Close()
	wg2.Wait()

	var totalConsumed = 0
	var duplicates = 0
	for _, c := range processedMessages {
		totalConsumed += c
		if c > 1 {
			duplicates+=(c-1)
		}
	}
	fmt.Println("Messages consumed:", totalConsumed)
	fmt.Println("Messages lost:", (nProducers*nMsgPerProducer)-len(processedMessages))
	fmt.Println("Messages duplicated:", duplicates)
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

func (q *Queue) Push(item Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && q.size == q.cap {
		q.condProd.Wait()
	}
	if q.closed {
		return
	}
	q.messages[q.tail] = item
	q.tail = (q.tail + 1) % q.cap
	q.size++
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
	item := q.messages[q.head]
	q.messages[q.head] = Message{}
	q.head = (q.head + 1) % q.cap
	q.size--
	q.condProd.Signal()
	return item, true
}

func (q *Queue) String() string {
	return fmt.Sprint(q.messages)
}
