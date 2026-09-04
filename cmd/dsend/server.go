package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
	"github.com/Ali-Hasan-Khan/dsend/internal/logger"
	"github.com/Ali-Hasan-Khan/dsend/internal/server"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

func runServer(args []string) error {
	wal, err := storage.NewFileWAL("./data/wal.log")
	if err != nil {
		return err
	}
	defer func() { _ = wal.Close() }()

	Logger := logger.GetInstance()

	cfg := engine.DefaultConfig()
	broker, err := engine.NewBroker(cfg, wal, Logger)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := server.New("127.0.0.1:8080", broker, Logger)

	broker.Start(ctx)

	if err := server.Start(ctx); err != nil {
		return err
	}

	broker.Shutdown()

	Logger.Infof("System shutdown successfully.")

	return nil
}
