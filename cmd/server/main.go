package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/Ali-Hasan-Khan/dsend/internal/broker"
	"github.com/Ali-Hasan-Khan/dsend/internal/model"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

const (
	nProducers      int = 50
	nConsumers      int = 50
	nMsgPerProducer int = 10
)

func main() {
	wal, err := storage.NewFileWAL("./data/wal.log")
	if err != nil {
		fmt.Println("Failed to create wal log file: ", err)
	}
	cfg := broker.DefaultConfig()
	cfg.QueueSize = 20
	b, err := broker.NewBroker(cfg, wal)
	if err != nil {
		log.Fatal(err)
	}

	deliveredMessages := make(map[string]int)
	ackMessages := make(map[string]int)
	var pLock sync.Mutex

	var producerWG, consumerWG, backgroundWG sync.WaitGroup

	for i := 1; i <= nProducers; i++ {
		producerWG.Add(1)
		go func(id int) {
			defer producerWG.Done()
			for msg := 1; msg <= nMsgPerProducer; msg++ {
				currID := (id-1)*nMsgPerProducer + msg
				msgStr := fmt.Sprintf("producer-%d-msg-%d", id, msg)
				message := model.Message{
					ID:        strconv.Itoa(currID),
					Payload:   []byte(msgStr),
					Timestamp: time.Now(),
					Retry:     0,
				}
				err := b.Publish(message)
				if err != nil {
					fmt.Println("Error pushing this message: ", err)
				}
			}
		}(i)
	}

	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()
		for {
			metrics := b.Metrics()

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
				value, ok := b.Consume()
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
		b.Start()
	}()

	producerWG.Wait()
	b.Stop()
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
	dlqSize := b.Metrics().DlqCount
	fmt.Println("Messages delivered:", totalDelivered)
	fmt.Println("Messages consumed:", totalAcks)
	fmt.Println("Messages lost:", (nProducers*nMsgPerProducer)-len(ackMessages))
	fmt.Println("Messages stored in DLQ:", dlqSize)
	fmt.Println("Messages duplicated:", duplicates)
}
