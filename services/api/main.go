package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	mux := http.NewServeMux()

	// ---------------------------------------------------------------------------
	// REST router
	// Wire in the REST handler here. Each resource group gets its own handler
	// func registered under /v1/. Example:
	//   mux.HandleFunc("GET /v1/events", handlers.ListEvents)
	//   mux.HandleFunc("GET /v1/events/{id}", handlers.GetEvent)
	// ---------------------------------------------------------------------------

	// ---------------------------------------------------------------------------
	// GraphQL handler
	// Mount the GraphQL endpoint here, e.g. using gqlgen:
	//   srv := handler.NewDefaultServer(generated.NewExecutableSchema(cfg))
	//   mux.Handle("/graphql", srv)
	//   mux.Handle("/playground", playground.Handler("Trident", "/graphql"))
	// ---------------------------------------------------------------------------

	// ---------------------------------------------------------------------------
	// WebSocket handler
	// Mount the WebSocket subscription endpoint here. Clients subscribe to a
	// contract address and receive a stream of SorobanEvent JSON objects in
	// real time as they land on-chain.
	//   mux.HandleFunc("/ws", ws.Handler(redisClient))
	// ---------------------------------------------------------------------------

	// ---------------------------------------------------------------------------
	// Redis Streams consumer
	// Start the background consumer here. It reads from the Redis Stream written
	// by the Rust indexer and fans out to connected WebSocket clients.
	//   go consumer.Start(ctx, redisClient, hub)
	// ---------------------------------------------------------------------------

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("Trident API server listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "err", err)
	}
}
