package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/i-segura/toy-raft/data"
	"github.com/i-segura/toy-raft/raft"
	"github.com/i-segura/toy-raft/raft/client"
	raftServer "github.com/i-segura/toy-raft/raft/server"
	"github.com/i-segura/toy-raft/raft/state"
	"github.com/i-segura/toy-raft/raft/store"
	"github.com/i-segura/toy-raft/server"
)

const electionTimeoutMaxJitter = 1.0

type peerCfg struct {
	host     string
	raftPort int
	httpPort int
}

type Arguments struct {
	raftPort int
	httpPort int
	peers    []peerCfg
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

	thisPeerID := peerID(strconv.Itoa(args.raftPort))
	log.Printf("starting peer %s", thisPeerID)

	st, err := store.New(fmt.Sprintf("state_%s.json", thisPeerID))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stt := state.New(ctx, st)

	dataURLs := map[string]string{}
	var peers []raft.PeerTuple
	for _, p := range args.peers {
		id := peerID(strconv.Itoa(p.raftPort))
		dataURLs[id] = fmt.Sprintf("http://%s:%d", p.host, p.httpPort)
		if p.raftPort == args.raftPort {
			continue
		}
		raftAddr := fmt.Sprintf("%s:%d", p.host, p.raftPort)
		peers = append(peers, raft.PeerTuple{
			ID:     id,
			Client: client.NewClient(raftAddr),
		})
	}

	jitter := int(rand.Float32() * electionTimeoutMaxJitter * 1000)

	node := raft.New(raft.NodeParams{
		ID:                     thisPeerID,
		ElectionTimeout:        time.Duration(10000+jitter) * time.Millisecond,
		LeaderHeartbeatTimeout: 5 * time.Second,
		Peers:                  peers,
		State:                  stt,
	})
	go node.Start()

	raftSrv := server.New(fmt.Sprintf("0.0.0.0:%d", args.raftPort), &raftServer.Handler{
		OnRequestVote:   node.HandleRequestVote,
		OnAppendEntries: node.HandleAppendEntry,
	})

	dataSrv := server.New(fmt.Sprintf("0.0.0.0:%d", args.httpPort), data.NewHandler(node, stt, dataURLs))

	setupSignals(raftSrv, dataSrv)

	go func() {
		if err := dataSrv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("data server: %v", err)
		}
	}()

	err = raftSrv.Start()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	rawPeers := strings.Split(peerList, ",")
	peers := make([]peerCfg, 0, len(rawPeers))
	for _, s := range rawPeers {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		p, err := parsePeerTriple(s)
		if err != nil {
			return nil, err
		}
		peers = append(peers, p)
	}

	return &Arguments{
		raftPort: int(raftPort),
		httpPort: int(httpPort),
		peers:    peers,
	}, nil
}

func parsePeerTriple(s string) (peerCfg, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return peerCfg{}, fmt.Errorf("peer %q must be host:raftPort:httpPort", s)
	}
	host := parts[0]
	raftP, err := strconv.Atoi(parts[1])
	if err != nil {
		return peerCfg{}, fmt.Errorf("peer %q: raft port: %w", s, err)
	}
	httpP, err := strconv.Atoi(parts[2])
	if err != nil {
		return peerCfg{}, fmt.Errorf("peer %q: http port: %w", s, err)
	}
	if raftP < 1024 || httpP < 1024 {
		return peerCfg{}, fmt.Errorf("peer %q: ports must be >= 1024", s)
	}
	return peerCfg{host: host, raftPort: raftP, httpPort: httpP}, nil
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

func peerID(port string) string {
	return fmt.Sprintf("peer_%s", port)
}
