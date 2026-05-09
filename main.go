package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/i-segura/toy-raft/raft"
	"github.com/i-segura/toy-raft/server"
)

type Arguments struct {
	raftPort int
	httpPort int
}

type CanBeStopped interface {
	Stop() error
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: raft <raft port> <http port>")
	}
	args, err := parseArguments()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Starting raft server on port %d", args.raftPort)
	raftServer := server.New(fmt.Sprintf("0.0.0.0:%d", args.raftPort), &raft.Handler{})

	setupSignals(raftServer)

	err = raftServer.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Server stopped")
}

func parseArguments() (*Arguments, error) {
	if len(os.Args) != 3 {
		return nil, errors.New("Usage: raft <raft port> <http port>")
	}
	raftPort, err := strconv.ParseInt(os.Args[1], 10, 16)
	if err != nil {
		return nil, err
	}
	if raftPort < 1024 {
		return nil, errors.New("raft port must be greater than 1024")
	}
	httpPort, err := strconv.ParseInt(os.Args[2], 10, 16)
	if err != nil {
		return nil, err
	}
	if httpPort < 1024 {
		return nil, errors.New("http port must be greater than 1024")
	}
	return &Arguments{
		raftPort: int(raftPort),
		httpPort: int(httpPort),
	}, nil
}

func setupSignals(s ...CanBeStopped) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("Shutting down")
		for _, s := range s {
			s.Stop()
		}
	}()
}
