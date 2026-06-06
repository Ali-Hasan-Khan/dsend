package main

import (
	"fmt"
	"sync"
)

const (
	nProducers int = 5
	nConsumers int = 3
)

type Queue struct {
	messages []string
	mu       sync.Mutex
	closed   bool
	cap      int

	condProd *sync.Cond
	condCons *sync.Cond
}

func main() {
	var queue = &Queue{
		messages: make([]string, 0),
		closed:   false,
		cap:      20,
	}

	queue.condProd = sync.NewCond(&queue.mu)
	queue.condCons = sync.NewCond(&queue.mu)

	processedMessages := make(map[string]int)
	var pLock sync.Mutex

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	for i := 1; i <= nProducers; i++ {
		wg1.Add(1)
		go func(id int) {
			defer wg1.Done()
			for msg := 1; msg <= 100; msg++ {
				msgStr := fmt.Sprintf("producer-%d-msg-%d", id, msg)
				queue.Push(msgStr)
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
				processedMessages[value] += 1
				pLock.Unlock()
			}
		}(i)
	}
	wg1.Wait()
	queue.Close()
	wg2.Wait()

	var totalConsumed = 0
	var hasDuplicates bool
	for _, c := range processedMessages {
		totalConsumed += c
		if c > 1 {
			hasDuplicates = true
		}
	}
	fmt.Println("Total messages consumed:", totalConsumed)
	fmt.Println("Duplicate messages detected:", hasDuplicates)
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
	var size = len(q.messages)
	return size
}

func (q *Queue) Push(item string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && len(q.messages) == q.cap {
		q.condProd.Wait()
	}
	if q.closed {
		return
	}
	q.messages = append(q.messages, item)
	q.condCons.Signal()
}

func (q *Queue) Pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.messages) == 0 && !q.closed {
		q.condCons.Wait()
	}
	if len(q.messages) == 0 && q.closed {
		return "", false
	}
	item := q.messages[0]
	q.messages = q.messages[1:]
	q.condProd.Signal()
	return item, true
}

func (q *Queue) String() string {
	return fmt.Sprint(q.messages)
}
