package raft

import (
	"sync"
	"time"

	"github.com/i-segura/toy-raft/raft/client"
	"github.com/i-segura/toy-raft/raft/protocol"
	"github.com/i-segura/toy-raft/raft/state"
	"github.com/i-segura/toy-raft/raft/store"
)

type Node struct {
	state state.State

	electionTimeout time.Duration
	electionTimer   time.Timer

	peers map[string]*client.Client

	mu sync.RWMutex
}

func New() *Node {
	return &Node{}
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
	for _, newEntry := range req.PrevLogTermEntries {
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
