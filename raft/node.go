package raft

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/i-segura/toy-raft/raft/client"
	"github.com/i-segura/toy-raft/raft/protocol"
	"github.com/i-segura/toy-raft/raft/state"
	"github.com/i-segura/toy-raft/raft/store"
)

// ErrNotLeader means this node cannot accept proposals in its current role.
var ErrNotLeader = errors.New("not leader")

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

	proposeMu sync.Mutex
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
				n.resetElectionTimer()
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
	accept, err := n.handleTerm(req.Term, req.LeaderID)
	if err != nil {
		return nil, &protocol.Error{
			Cause: "state error",
		}
	}
	if !accept {
		return &protocol.AppendEntriesResponse{
			Term:    n.state.Snapshot().CurrentTerm,
			Success: false,
		}, nil
	}

	state := n.state.Snapshot()
	n.resetElectionTimer()

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
			Term:    newEntry.Term,
			Command: newEntry.Command,
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
	accept, err := n.handleTerm(req.Term, "")
	if err != nil {
		return nil, &protocol.Error{
			Cause: "state error",
		}
	}
	if !accept {
		return &protocol.RequestVoteResponse{
			Term:        n.state.Snapshot().CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	state := n.state.Snapshot()

	votedForNullOrCandidateID := state.VotedFor == "" || state.VotedFor == req.CandidateID
	localLastLogIndex := n.state.LogLen() - 1
	localLastLogTerm := n.state.EntryTerm(localLastLogIndex)
	candidateUpToData := req.LastLogTerm > localLastLogTerm || (req.LastLogTerm == localLastLogTerm && req.LastLogIndex >= localLastLogIndex)
	grantVote := votedForNullOrCandidateID && candidateUpToData
	if grantVote {
		err := n.state.CastVote(req.Term, req.CandidateID)
		n.resetElectionTimer()
		if err != nil {
			return nil, &protocol.Error{
				Cause: "state error",
			}
		}

		log.Printf("%s voted for %s", n.id, req.CandidateID)
	} else {
		log.Printf("%s vote denied for %s", n.id, req.CandidateID)
	}

	return &protocol.RequestVoteResponse{
		Term:        state.CurrentTerm,
		VoteGranted: grantVote,
	}, nil
}

// Propose appends cmd on the leader, replicates to a quorum, advances commit, and returns the
// 0-based log index of the new entry. Serializes with leader heartbeats via proposeMu.
func (n *Node) Propose(ctx context.Context, cmd any) (int, error) {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()

	snap := n.state.Snapshot()
	if snap.Role != state.Leader {
		return 0, ErrNotLeader
	}

	newIdx := n.state.LogLen()
	term := snap.CurrentTerm
	leaderCommit := snap.CommitIndex

	if err := n.state.AppendEntries(leaderCommit, newIdx, store.LogEntry{
		Term:    term,
		Command: cmd,
	}); err != nil {
		return 0, err
	}

	total := len(n.peers) + 1
	majority := total/2 + 1

	var rep atomic.Int32
	rep.Store(1)

	var repMu sync.Mutex
	steppedDown := false
	stepDownTerm := 0

	var wg sync.WaitGroup
	for fid, fpeer := range n.peers {
		wg.Add(1)
		go func(id string, peer *client.Client) {
			defer wg.Done()
			if n.replicateEntryToFollower(ctx, id, peer, newIdx, leaderCommit, &repMu, &steppedDown, &stepDownTerm) {
				rep.Add(1)
			}
		}(fid, fpeer)
	}
	wg.Wait()

	repMu.Lock()
	sd := steppedDown
	st := stepDownTerm
	repMu.Unlock()

	if sd {
		if err := n.state.BecomeFollower("", st); err != nil {
			log.Printf("%s step down error: %v", n.id, err)
		}
		n.resetElectionTimer()
		return 0, ErrNotLeader
	}

	if int(rep.Load()) < majority {
		return 0, errors.New("replication quorum not reached")
	}

	if err := n.state.AppendEntries(newIdx+1, newIdx+1); err != nil {
		return 0, err
	}

	return newIdx, nil
}

func (n *Node) heartbeatFollowers() {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()

	state := n.state.LeaderSnapshot()
	logLen := n.state.LogLen()

	wg := sync.WaitGroup{}
	for id, peer := range n.peers {
		peerNextIdx, ok := state.LeaderNextIndex[id]
		if !ok {
			peerNextIdx = logLen
		}

		peerTerm := n.state.EntryTerm(peerNextIdx - 1)
		wg.Go(func() {
			res, err := peer.AppendEntries(protocol.AppendEntriesRequest{
				Term:         state.CurrentTerm,
				LeaderID:     n.id,
				PrevLogIndex: peerNextIdx - 1,
				PrevLogTerm:  peerTerm,
				LeaderCommit: state.CommitIndex,
				Entries:      []protocol.AppendEntry{},
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
				n.resetElectionTimer()
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
	n.resetElectionTimer()

	state := n.state.Snapshot()
	if len(n.peers) == 0 {
		n.state.BecomeLeader(n.id)
		log.Printf("%s became leader (single node)", n.id)
		n.heartbeatFollowers()
		return
	}
	localLastLogIndex := n.state.LogLen() - 1
	localLastLogTerm := n.state.EntryTerm(localLastLogIndex)

	total := len(n.peers) + 1

	var rep atomic.Int32
	rep.Store(1)

	wg := sync.WaitGroup{}

	for id, peer := range n.peers {
		wg.Go(func() {
			res, err := peer.RequestVote(protocol.RequestVoteRequest{
				Term:         state.CurrentTerm,
				CandidateID:  n.id,
				LastLogIndex: localLastLogIndex,
				LastLogTerm:  localLastLogTerm,
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

			if res.VoteGranted {
				rep.Add(1)
			}

		})
	}

	wg.Wait()

	majority := total/2 + 1
	if int(rep.Load()) >= majority {
		n.state.BecomeLeader(n.id)
		log.Printf("%s became leader", n.id)
		n.heartbeatFollowers()
	} else {
		log.Printf("%s candidate did not receive enough votes: %d/%d", n.id, rep.Load(), majority)
	}

}

func (n *Node) replicateEntryToFollower(ctx context.Context, id string, peer *client.Client, newIdx, leaderCommit int, repMu *sync.Mutex, steppedDown *bool, stepDownTerm *int) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		repMu.Lock()
		if *steppedDown {
			repMu.Unlock()
			return false
		}
		repMu.Unlock()

		ls := n.state.LeaderSnapshot()
		term := ls.CurrentTerm

		peerNextIdx, ok := ls.LeaderNextIndex[id]
		if !ok || peerNextIdx > newIdx {
			peerNextIdx = newIdx
		}
		if peerNextIdx < 0 {
			peerNextIdx = 0
		}

		prevLogIndex := peerNextIdx - 1
		prevLogTerm := n.state.EntryTerm(prevLogIndex)
		entries := n.state.LogCommandsRange(peerNextIdx, newIdx)
		if len(entries) != newIdx-peerNextIdx+1 {
			return false
		}

		res, err := peer.AppendEntries(protocol.AppendEntriesRequest{
			Term:         term,
			LeaderID:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: leaderCommit,
			Entries:      entries,
		})
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}

		if res.Term > n.state.Snapshot().CurrentTerm {
			repMu.Lock()
			if !*steppedDown || res.Term > *stepDownTerm {
				*steppedDown = true
				*stepDownTerm = res.Term
			}
			repMu.Unlock()
			return false
		}

		if res.Success {
			n.state.SetPeerCurrentIndex(id, newIdx)
			n.state.SetPeerNextIndex(id, newIdx+1)
			return true
		}

		nextDec := peerNextIdx - 1
		if nextDec < 0 {
			nextDec = 0
		}
		n.state.SetPeerNextIndex(id, nextDec)
	}
}

// Handle received term from request. Return false if it should be rejected.
func (n *Node) handleTerm(peerTerm int, leaderOrNone string) (bool, error) {
	state := n.state.Snapshot()
	if peerTerm < state.CurrentTerm {
		log.Printf("%s request denied: current term %d, theirs %d", n.id, state.CurrentTerm, peerTerm)
		return false, nil
	} else if peerTerm > state.CurrentTerm {
		log.Printf("%s request term update: become follower, current term %d, theirs %d", n.id, state.CurrentTerm, peerTerm)
		return true, n.state.BecomeFollower(leaderOrNone, peerTerm)
	}
	if leaderOrNone != "" {
		n.state.UpdateLeader(leaderOrNone)
	}
	return true, nil
}

func (n *Node) resetElectionTimer() {
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(n.electionTimeout)
}
