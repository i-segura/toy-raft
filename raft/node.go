package raft

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/i-segura/toy-raft/raft/client"
	"github.com/i-segura/toy-raft/raft/protocol"
	"github.com/i-segura/toy-raft/raft/state"
	"github.com/i-segura/toy-raft/raft/store"
)

type Node struct {
	id    string
	state state.State

	electionTimeout time.Duration
	electionTimer   time.Timer

	peers map[string]*client.Client

	leaderHeartbeatTimeout time.Duration
	leaderHeartbeatTimer   time.Ticker

	mu sync.RWMutex
}

func New() *Node {
	return &Node{}
}

func (n *Node) Start() {
	for {
		select {
		case <-n.electionTimer.C:
			if n.state.GetRole() == state.Leader {
				n.electionTimer.Reset(n.electionTimeout)
			}
		case <-n.leaderHeartbeatTimer.C:
			n.heartbeatFollowers()
		}
	}
}

func (n *Node) HandleAppendEntry(req protocol.AppendEntriesRequest) (*protocol.AppendEntriesResponse, *protocol.Error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.state.CurrentTerm()
	if req.Term < currentTerm {
		return &protocol.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}, nil
	}

	if req.Term > currentTerm {
		err := n.state.SetTerm(req.Term)
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}
		currentTerm = req.Term
	}

	n.state.SetCurrentLeader(req.LeaderID)
	n.state.SetRole(state.Follower)
	n.electionTimer.Reset(n.electionTimeout)

	reqEntryTerm := n.state.EntryTerm(req.PrevLogIndex)
	if reqEntryTerm < 0 || reqEntryTerm != req.PrevLogTerm {
		return &protocol.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}, nil
	}

	logEntries := []store.LogEntry{}
	for _, newEntry := range req.Entries {
		logEntries = append(logEntries, store.LogEntry{
			Term:    currentTerm,
			Command: newEntry,
		})
	}

	lastIndex, err := n.state.AppendEntries(req.PrevLogIndex+1, logEntries...)
	if err != nil {
		return nil, &protocol.Error{
			Cause: "state error",
		}
	}

	if req.LeaderCommit > n.state.CommitIndex() {
		n.state.SetCommitIndex(min(req.LeaderCommit, lastIndex))
	}

	return &protocol.AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}, nil
}

func (n *Node) HandleRequestVote(req protocol.RequestVoteRequest) (*protocol.RequestVoteResponse, *protocol.Error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.state.CurrentTerm()
	if req.Term < currentTerm {
		return &protocol.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}, nil
	}

	if req.Term > currentTerm {
		err := n.state.SetTerm(req.Term)
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}
		currentTerm = req.Term
	}

	votedFor := n.state.VotedFor()
	votedForNullOrCandidateID := votedFor == "" || votedFor == req.CandidateID
	grantVote := votedForNullOrCandidateID && n.state.CommitIndex() <= req.LastLogIndex
	if grantVote {
		err := n.state.VoteFor(req.CandidateID)
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}
		n.electionTimer.Reset(n.electionTimeout)
	}

	return &protocol.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: grantVote,
	}, nil
}

func (n *Node) heartbeatFollowers() {

	wg := sync.WaitGroup{}
	for id, peer := range n.peers {
		peerNextIdx := n.state.GetPeerNextIndex(id)
		peerTerm := n.state.EntryTerm(peerNextIdx - 1)
		if peerTerm < 0 {
			fmt.Printf("peer entry not found")
			continue
		}

		wg.Go(func() {
			res, err := peer.AppendEntries(protocol.AppendEntriesRequest{
				Term:         n.state.CurrentTerm(),
				LeaderID:     n.id,
				PrevLogIndex: peerNextIdx - 1,
				PrevLogTerm:  peerTerm,
				LeaderCommit: n.state.CommitIndex(),
				Entries:      []any{},
			})
			if err != nil {
				log.Printf("error appending entries: %w", err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if res.Term > n.state.CurrentTerm() {
				n.state.SetCurrentLeader("")
				n.state.SetRole(state.Follower)
				n.electionTimer.Reset(n.electionTimeout)
			}

			if !res.Success {
				n.state.SetPeerNextIndex(id, peerNextIdx-1)
			}
		})
	}

	wg.Wait()
}

func (n *Node) startElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.SetTerm(n.state.CurrentTerm() + 1)
	n.state.SetRole(state.Candidate)
	n.state.VoteFor(n.id)
	n.electionTimer.Reset(n.electionTimeout)

	responses := map[string]bool{}

	wg := sync.WaitGroup{}
	for id, peer := range n.peers {
		wg.Go(func() {
			res, err := peer.RequestVote(protocol.RequestVoteRequest{
				Term:         n.state.CurrentTerm(),
				CandidateID:  n.id,
				LastLogIndex: n.state.CommitIndex(),
				LastLogTerm:  n.state.EntryTerm(n.state.CurrentTerm() - 1),
			})
			if err != nil {
				log.Printf("error requesting vote: %w", err)
				return
			}

			if res.Term > n.state.CurrentTerm() {
				n.state.SetCurrentLeader("")
				n.state.SetRole(state.Follower)
			}

			responses[id] = res.VoteGranted
		})
	}

	quorum := 0
	for _, granted := range responses {
		if !granted {
			continue
		}
		quorum++
		if quorum > len(n.peers)/2 {
			n.state.SetCurrentLeader(n.id)
			n.state.SetRole(state.Leader)
			n.mu.Unlock()
			n.heartbeatFollowers()
		}
	}
}
