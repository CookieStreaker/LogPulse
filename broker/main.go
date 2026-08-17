package main

import (
	"context"
	"fmt"
	"log"
	"mini-kafka/api"
	"mini-kafka/consumer"
	"mini-kafka/network"
	"mini-kafka/topic"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	dataDir := envOr("DATA_DIR", "./data")
	tcpAddr := envOr("TCP_ADDR", ":9092")
	httpAddr := envOr("HTTP_ADDR", ":8080")
	maxSegSize := int64(10 * 1024 * 1024) // 10MB

	fmt.Println("=====================================")
	fmt.Println("       STARTING MINI-KAFKA           ")
	fmt.Println("=====================================")
	fmt.Printf("Data Dir: %s\n", dataDir)
	fmt.Printf("TCP Addr: %s\n", tcpAddr)
	fmt.Printf("HTTP Addr: %s\n", httpAddr)

	topicMgr, err := topic.NewManager(dataDir, maxSegSize)
	if err != nil {
		log.Fatalf("Failed to initialize topic manager: %v", err)
	}

	consumerMgr, err := consumer.NewGroupManager(dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize consumer manager: %v", err)
	}

	tcpServer := network.NewServer(tcpAddr, topicMgr, consumerMgr)
	if err := tcpServer.Start(); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}

	httpServer := api.NewHTTPServer(httpAddr, topicMgr, consumerMgr)
	go func() {
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	fmt.Println("mini-kafka broker-1 is up and running!")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	fmt.Println("\nShutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	if err := tcpServer.Stop(); err != nil {
		log.Printf("TCP server shutdown error: %v", err)
	}

	if err := topicMgr.Close(); err != nil {
		log.Printf("Topic manager shutdown error: %v", err)
	}

	fmt.Println("Shutdown complete.")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
