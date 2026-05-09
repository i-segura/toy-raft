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
		s := n.state.Snapshot()
		select {
		case <-n.electionTimer.C:
			if s.Role == state.Leader {
				n.electionTimer.Reset(n.electionTimeout)
				continue
			}
			n.startElection()
		case <-n.leaderHeartbeatTimer.C:
			if s.Role != state.Leader {
				continue
			}
			n.heartbeatFollowers()
		}
	}
}

func (n *Node) HandleAppendEntry(req protocol.AppendEntriesRequest) (*protocol.AppendEntriesResponse, *protocol.Error) {
	state := n.state.Snapshot()
	if req.Term < state.CurrentTerm {
		log.Printf("%s request denied: current term %d, their term %d", state.CurrentTerm, req.Term)
		return &protocol.AppendEntriesResponse{
			Term:    state.CurrentTerm,
			Success: false,
		}, nil
	}

	err := n.state.BecomeFollower(req.LeaderID, req.Term)
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
		log.Printf("%s request denied: current term %d, their term %d", state.CurrentTerm, req.Term)
		return &protocol.RequestVoteResponse{
			Term:        state.CurrentTerm,
			VoteGranted: false,
		}, nil
	}
	if req.Term > state.CurrentTerm {
		state.CurrentTerm = req.Term
	}

	votedForNullOrCandidateID := state.VotedFor == "" || state.VotedFor == req.CandidateID
	grantVote := votedForNullOrCandidateID && state.CommitIndex <= req.LastLogIndex
	if grantVote {
		err := n.state.CastVote(req.Term, req.CandidateID)
		n.electionTimer.Reset(n.electionTimeout)
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}

		log.Printf("%s voted for %s", n.id, req.CandidateID)
	} else {
		// NOTE: Force term update.
		err := n.state.CastVote(req.Term, "")
		n.electionTimer.Reset(n.electionTimeout)
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}
		log.Printf("%s vote denied for %s", n.id, req.CandidateID)
	}

	return &protocol.RequestVoteResponse{
		Term:        state.CurrentTerm,
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
				log.Printf("error appending entries: %s", err)
				return
			}
			log.Printf("%s append entry response", n.id)

			if res.Term > state.CurrentTerm {
				err := n.state.BecomeFollower("", res.Term)
				if err != nil {
					log.Printf("error setting current term: %s", err)
					return
				}
				n.electionTimer.Reset(n.electionTimeout)
				log.Printf("%s became follower", n.id)
			}

			if !res.Success {
				n.state.SetPeerNextIndex(id, peerNextIdx-1)
			}
		})
	}

	wg.Wait()
}

func (n *Node) startElection() {
	log.Printf("%s start election", n.id)

	err := n.state.BecomeCandidate(n.id)
	if err != nil {
		log.Printf("error starting election: %s", err)
	}
	n.electionTimer.Reset(n.electionTimeout)

	state := n.state.Snapshot()
	lastCommitTerm := n.state.EntryTerm(state.CommitIndex - 1)
	responses := map[string]bool{}

	for id, peer := range n.peers {
		res, err := peer.RequestVote(protocol.RequestVoteRequest{
			Term:         state.CurrentTerm,
			CandidateID:  n.id,
			LastLogIndex: state.CommitIndex,
			LastLogTerm:  lastCommitTerm,
		})
		if err != nil {
			log.Printf("error requesting vote: %s", err)
			return
		}
		log.Printf("%s vote response from %s", n.id, id)

		if res.Term > state.CurrentTerm {
			err := n.state.BecomeFollower("", res.Term)
			if err != nil {
				log.Printf("error setting current term: %s", err)
				return
			}
			log.Printf("%s became follower", n.id)
			return
		}

		responses[id] = res.VoteGranted
	}

	quorum := 0
	for _, granted := range responses {
		if !granted {
			continue
		}
		quorum++
		if quorum >= len(n.peers)/2 {
			n.state.BecomeLeader(n.id)
			log.Printf("%s became leader", n.id)
			n.heartbeatFollowers()
			return
		}
	}
	log.Printf("%s candidate did not receive enough votes: %d/%d", n.id, quorum, len(n.peers)/2)
}
