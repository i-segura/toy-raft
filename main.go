package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/i-segura/toy-raft/raft"
	"github.com/i-segura/toy-raft/raft/client"
	raftServer "github.com/i-segura/toy-raft/raft/server"
	"github.com/i-segura/toy-raft/raft/state"
	"github.com/i-segura/toy-raft/raft/store"
	"github.com/i-segura/toy-raft/server"
)

const electionTimeoutMaxJitter = 1.0

type Arguments struct {
	raftPort int
	httpPort int
	peers    []string
}

type CanBeStopped interface {
	Stop() error
}

func main() {
	args, err := parseArguments()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Starting raft server on port %d", args.raftPort)

	thisPeerId := peerId(strconv.FormatInt(int64(args.raftPort), 10))
	log.Printf("starting peer %s", thisPeerId)

	store, err := store.New(fmt.Sprintf("state_%s.json", thisPeerId))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	state := state.New(ctx, store)

	peers := []raft.PeerTuple{}
	for _, peerAddr := range args.peers {
		peerPort := strings.Split(peerAddr, ":")[1]
		peers = append(peers, raft.PeerTuple{
			ID:     peerId(peerPort),
			Client: client.NewClient(peerAddr),
		})
	}

	jitter := int(rand.Float32() * electionTimeoutMaxJitter * 1000)

	node := raft.New(raft.NodeParams{
		ID:                     thisPeerId,
		ElectionTimeout:        time.Duration(10000+jitter) * time.Millisecond,
		LeaderHeartbeatTimeout: 5 * time.Second,
		Peers:                  peers,
		State:                  state,
	})
	go node.Start()

	raftServer := server.New(fmt.Sprintf("0.0.0.0:%d", args.raftPort), &raftServer.Handler{
		OnRequestVote:   node.HandleRequestVote,
		OnAppendEntries: node.HandleAppendEntry,
	})

	setupSignals(raftServer)

	err = raftServer.Start()
	if err != nil {
		log.Fatal(err)
	}
	cancel()
	log.Println("Server stopped")
}

func parseArguments() (*Arguments, error) {
	if len(os.Args) != 4 {
		return nil, errors.New("Usage: raft <raft port> <http port> <peer list>")
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

	peerList := os.Args[3]

	return &Arguments{
		raftPort: int(raftPort),
		httpPort: int(httpPort),
		peers:    strings.Split(peerList, ","),
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

func peerId(port string) string {
	return fmt.Sprintf("peer_%s", port)
}
