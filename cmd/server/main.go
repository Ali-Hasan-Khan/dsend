package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
	"github.com/Ali-Hasan-Khan/dsend/internal/server"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

func main() {
	wal, err := storage.NewFileWAL("./data/wal.log")
	if err != nil {
		log.Println("Failed to create wal log file: ", err)
	}
	cfg := engine.DefaultConfig()
	broker, err := engine.NewBroker(cfg, wal)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := server.New("127.0.0.1:8080", broker)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		broker.StartRedeliveryWorker(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		broker.RunDistributor(ctx)
	}()

	if err := server.Start(ctx); err != nil {
		log.Fatal(err)
	}

	broker.Shutdown()

	log.Println("Broker shutdown successfully. Waiting for workers to shutdown...")

	wg.Wait()

	log.Println("System shutdown successfully.")
}
