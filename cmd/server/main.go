package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Ali-Hasan-Khan/dsend/internal/broker"
	"github.com/Ali-Hasan-Khan/dsend/internal/server"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

func main() {
	wal, err := storage.NewFileWAL("./data/wal.log")
	if err != nil {
		log.Println("Failed to create wal log file: ", err)
	}
	cfg := broker.DefaultConfig()
	cfg.QueueSize = 20
	broker, err := broker.NewBroker(cfg, wal)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := server.New(":8080", broker)

	var wg sync.WaitGroup

	wg.Go(func() {
		broker.StartRedeliveryWorker()
	})

	if err := server.Start(ctx); err != nil {
		log.Fatal(err)
	}
	broker.Shutdown()
	wg.Wait()

	log.Println("Broker shutdown successfully...")
}
