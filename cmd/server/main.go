package main

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/broker"
)

const (
	nProducers      int = 50
	nConsumers      int = 50
	queueCapacity   int = 20
	nMsgPerProducer int = 10
)

func main() {
	b := broker.NewBroker(queueCapacity)

	deliveredMessages := make(map[string]int)
	ackMessages := make(map[string]int)
	var pLock sync.Mutex

	var producerWG sync.WaitGroup
	var consumerWG sync.WaitGroup
	var backgroundWG sync.WaitGroup

	for i := 1; i <= nProducers; i++ {
		producerWG.Add(1)
		go func(id int) {
			defer producerWG.Done()
			for msg := 1; msg <= nMsgPerProducer; msg++ {
				currID := (id-1)*nMsgPerProducer + msg
				msgStr := fmt.Sprintf("producer-%d-msg-%d", id, msg)
				message := broker.Message{
					ID:        strconv.Itoa(currID),
					Payload:   []byte(msgStr),
					Timestamp: time.Now(),
					Retry:     0,
				}
				b.Push(message)
			}
		}(i)
	}

	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()
		for {
			metrics := b.GetMetrics()

			isClosed := b.IsClosed()

			fmt.Printf("Acked count: %d\033[K\n", metrics.AckedCount)
			fmt.Printf("Inflight:    %d\033[K\n", metrics.InflightCount)

			// If the queue is finished, exit the print loop
			if isClosed {
				break
			}

			time.Sleep(100 * time.Millisecond)

			fmt.Print("\033[F\033[F")
		}
	}()

	for i := 1; i <= nConsumers; i++ {
		consumerWG.Add(1)
		go func(id int) {
			defer consumerWG.Done()
			for {
				value, ok := b.Pop()
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

				b.Ack(value.AckToken)

				pLock.Lock()
				ackMessages[value.ID]++
				pLock.Unlock()
			}
		}(i)
	}

	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()
		for !b.IsClosed() {
			time.Sleep(time.Second * 2)
			b.StartRedeliveryLoop()
		}
	}()

	producerWG.Wait()
	for {

		if b.IsDone() {
			break
		}

		time.Sleep(time.Second)
	}
	b.Close()
	consumerWG.Wait()
	backgroundWG.Wait()
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
	fmt.Println("Messages stored in DLQ:", b.GetDLQSize())
	fmt.Println("Messages duplicated:", duplicates)
}
