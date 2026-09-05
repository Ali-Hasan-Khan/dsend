package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ali-Hasan-Khan/dsend/internal/engine"
	"github.com/Ali-Hasan-Khan/dsend/internal/logger"
	"github.com/Ali-Hasan-Khan/dsend/internal/server"
	"github.com/Ali-Hasan-Khan/dsend/internal/storage"
)

func runServer(args []string) error {
	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	addr := serverCmd.String("addr", "127.0.0.1:8080", "server address")
	walPath := serverCmd.String("wal", "./data/wal.log", "WAL path")

	if err := serverCmd.Parse(args); err != nil {
		return err
	}

	wal, err := storage.NewFileWAL(*walPath)
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

	server := server.New(*addr, broker, Logger)

	broker.Start(ctx)

	if err := server.Start(ctx); err != nil {
		return err
	}

	broker.Shutdown()

	Logger.Infof("System shutdown successfully.")

	return nil
}
