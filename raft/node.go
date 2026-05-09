package raft

import (
	"log"
	"sync"
	"time"

	"github.com/i-segura/toy-raft/raft/client"
	"github.com/i-segura/toy-raft/raft/protocol"
	"github.com/i-segura/toy-raft/raft/state"
	"github.com/i-segura/toy-raft/raft/store"
)

type PeerTuple struct {
	ID     string
	Client *client.Client
}

type NodeParams struct {
	ID                     string
	ElectionTimeout        time.Duration
	LeaderHeartbeatTimeout time.Duration
	Peers                  []PeerTuple
	State                  *state.State
}

type Node struct {
	id    string
	state *state.State

	peers map[string]*client.Client

	electionTimeout time.Duration

	electionTimer        *time.Timer
	leaderHeartbeatTimer *time.Ticker
}

func New(params NodeParams) *Node {
	peerMap := map[string]*client.Client{}
	for _, peerTuple := range params.Peers {
		peerMap[peerTuple.ID] = peerTuple.Client
	}

	return &Node{
		id:    params.ID,
		state: params.State,

		peers: peerMap,

		electionTimeout: params.ElectionTimeout,

		electionTimer:        time.NewTimer(params.ElectionTimeout),
		leaderHeartbeatTimer: time.NewTicker(params.LeaderHeartbeatTimeout),
	}
}

func (n *Node) Start() {
	for {
		select {
		case <-n.electionTimer.C:
			if n.state.Snapshot().Role == state.Leader {
				n.electionTimer.Reset(n.electionTimeout)
			}
			n.startElection()
		case <-n.leaderHeartbeatTimer.C:
			n.heartbeatFollowers()
		}
	}
}

func (n *Node) HandleAppendEntry(req protocol.AppendEntriesRequest) (*protocol.AppendEntriesResponse, *protocol.Error) {
	state := n.state.Snapshot()
	if req.Term < state.CurrentTerm {
		return &protocol.AppendEntriesResponse{
			Term:    state.CurrentTerm,
			Success: false,
		}, nil
	}

	err := n.state.BecomeFollower(req.Term)
	if err != nil {
		return nil, &protocol.Error{
			Cause: "state error",
		}
	}
	n.electionTimer.Reset(n.electionTimeout)
	if req.Term > state.CurrentTerm {
		state.CurrentTerm = req.Term
	}

	reqEntryTerm := n.state.EntryTerm(req.PrevLogIndex)
	if reqEntryTerm != req.PrevLogTerm {
		return &protocol.AppendEntriesResponse{
			Term:    state.CurrentTerm,
			Success: false,
		}, nil
	}

	logEntries := []store.LogEntry{}
	for _, newEntry := range req.Entries {
		logEntries = append(logEntries, store.LogEntry{
			Term:    state.CurrentTerm,
			Command: newEntry,
		})
	}

	err = n.state.AppendEntries(req.LeaderCommit, req.PrevLogIndex+1, logEntries...)
	if err != nil {
		return nil, &protocol.Error{
			Cause: "state error",
		}
	}

	return &protocol.AppendEntriesResponse{
		Term:    state.CurrentTerm,
		Success: true,
	}, nil
}

func (n *Node) HandleRequestVote(req protocol.RequestVoteRequest) (*protocol.RequestVoteResponse, *protocol.Error) {
	state := n.state.Snapshot()
	if req.Term < state.CurrentTerm {
		return &protocol.RequestVoteResponse{
			Term:        state.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	votedForNullOrCandidateID := state.VotedFor == "" || state.VotedFor == req.CandidateID
	grantVote := votedForNullOrCandidateID && state.CommitIndex <= req.LastLogIndex
	if grantVote {
		err := n.state.CastVote(req.Term, req.CandidateID)
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}
		n.electionTimer.Reset(n.electionTimeout)
	} else {
		// NOTE: Force term update.
		err := n.state.CastVote(req.Term, state.VotedFor)
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}
	}

	return &protocol.RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: grantVote,
	}, nil
}

func (n *Node) heartbeatFollowers() {
	state := n.state.LeaderSnapshot()

	wg := sync.WaitGroup{}
	for id, peer := range n.peers {
		peerNextIdx, ok := state.LeaderNextIndex[id]
		if !ok {
			peerNextIdx = 1
		}

		peerTerm := n.state.EntryTerm(peerNextIdx - 1)
		wg.Go(func() {
			res, err := peer.AppendEntries(protocol.AppendEntriesRequest{
				Term:         state.CurrentTerm,
				LeaderID:     n.id,
				PrevLogIndex: peerNextIdx - 1,
				PrevLogTerm:  peerTerm,
				LeaderCommit: state.CommitIndex,
				Entries:      []any{},
			})
			if err != nil {
				log.Printf("error appending entries: %w", err)
				return
			}

			if res.Term > state.CurrentTerm {
				err := n.state.BecomeFollower(res.Term)
				if err != nil {
					log.Printf("error setting current term: %w", err)
					return
				}
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

	err := n.state.BecomeCandidate(n.id)
	if err != nil {
		log.Printf("error starting election: %w", err)
	}
	n.electionTimer.Reset(n.electionTimeout)

	state := n.state.Snapshot()
	lastCommitTerm := n.state.EntryTerm(state.CommitIndex - 1)
	responses := map[string]bool{}

	wg := sync.WaitGroup{}
	for id, peer := range n.peers {
		wg.Go(func() {
			res, err := peer.RequestVote(protocol.RequestVoteRequest{
				Term:         state.CurrentTerm,
				CandidateID:  n.id,
				LastLogIndex: state.CommitIndex,
				LastLogTerm:  lastCommitTerm,
			})
			if err != nil {
				log.Printf("error requesting vote: %w", err)
				return
			}

			if res.Term > state.CurrentTerm {
				err := n.state.BecomeFollower(res.Term)
				if err != nil {
					log.Printf("error setting current term: %w", err)
					return
				}
			}

			responses[id] = res.VoteGranted
		})
	}
	wg.Wait()

	quorum := 0
	for _, granted := range responses {
		if !granted {
			continue
		}
		quorum++
		if quorum > len(n.peers)/2 {
			log.Printf("%s became leader", n.id)
			n.state.BecomeLeader(n.id)
			n.heartbeatFollowers()
		}
	}
}
