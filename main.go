package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/i-segura/toy-raft/server"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: raft <port>")
	}
	portStr := os.Args[1]
	port, err := strconv.ParseInt(portStr, 10, 16)
	if err != nil {
		log.Fatal(err)
	}
	if port < 1024 {
		log.Fatal("Port must be greater than 1024")
	}

	log.Println("Starting server on port 8080")
	s := server.New(fmt.Sprintf("0.0.0.0:%d", port))
	setupSignals(s)

	err = s.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Server stopped")
}

func setupSignals(s *server.Server) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("Shutting down server")
		s.Stop()
	}()
}
