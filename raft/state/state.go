package state

import (
	"context"
	"maps"

	"github.com/i-segura/toy-raft/raft/store"
)

type Role int

const Follower Role = 1
const Candidate Role = 2
const Leader Role = 3

type State struct {
	role Role

	currentLeader string
	commitIndex   int

	leaderNextIndex  map[string]int
	leaderMatchIndex map[string]int

	store *store.Store // Contains log and persistent state.

	opCh chan<- operation
}

type StateSnapshot struct {
	Role          Role
	CurrentLeader string // Current leader. Empty if unknown.
	CommitIndex   int    // Index of highest log entry known to be commited.
	CurrentTerm   int    // Latest term server has seen.
	VotedFor      string // Who received the vote in current term. Empty if none.

}

type LeaderSnapShot struct {
	StateSnapshot
	LeaderNextIndex  map[string]int // Index of the next log to send to each server.
	LeaderMatchIndex map[string]int // Index of highest log entry known replicated on each server.
}

func New(ctx context.Context, store *store.Store) *State {
	opCh := make(chan operation)
	s := &State{
		role:             Follower,
		currentLeader:    "",
		commitIndex:      0,
		leaderNextIndex:  map[string]int{},
		leaderMatchIndex: map[string]int{},
		store:            store,
		opCh:             opCh,
	}

	go func() {
		for {
			select {
			case op := <-opCh:
				s.handleOperation(op)
			case <-ctx.Done():
				return
			}
		}
	}()

	return s
}

func (s *State) Snapshot() StateSnapshot {
	store := s.store.Snapshot()
	return StateSnapshot{
		Role:          s.role,
		CurrentLeader: s.currentLeader,
		CommitIndex:   s.commitIndex,
		CurrentTerm:   store.CurrentTerm,
		VotedFor:      store.VotedFor,
	}
}

func (s *State) LeaderSnapshot() LeaderSnapShot {
	return LeaderSnapShot{
		StateSnapshot:    s.Snapshot(),
		LeaderNextIndex:  maps.Clone(s.leaderNextIndex),
		LeaderMatchIndex: maps.Clone(s.leaderMatchIndex),
	}
}

// Get log entry's term. 0 if not found.
func (s *State) EntryTerm(idx int) int {
	snap := s.store.Snapshot()

	if idx < 0 || len(snap.Log) <= idx {
		return 0
	}

	return snap.Log[idx].Term
}

func (s *State) BecomeFollower(leader string, term int) error {
	errCh := make(chan error)

	s.opCh <- operation{
		type_:  BecomeFollower,
		term:   term,
		peerId: leader,
		errCh:  errCh,
	}

	return <-errCh
}

func (s *State) BecomeCandidate(id string) error {
	errCh := make(chan error)

	s.opCh <- operation{
		type_:  BecomeCandidate,
		peerId: id,
		errCh:  errCh,
	}

	return <-errCh
}

func (s *State) BecomeLeader(id string) {
	errCh := make(chan error)

	s.opCh <- operation{
		type_:  BecomeLeader,
		peerId: id,
		errCh:  errCh,
	}

	<-errCh
}

// Updated voted for candidate this term. Reset election timeout.
func (s *State) CastVote(term int, candidate string) error {
	errCh := make(chan error)

	s.opCh <- operation{
		type_:  CastVote,
		term:   term,
		peerId: candidate,
		errCh:  errCh,
	}

	return <-errCh
}

func (s *State) SetPeerNextIndex(peer string, idx int) {
	errCh := make(chan error)

	s.opCh <- operation{
		type_:  SetPeerNextIndex,
		peerId: peer,
		index:  idx,
		errCh:  errCh,
	}

	<-errCh
}

func (s *State) SetPeerCurrentIndex(peer string, idx int) {
	errCh := make(chan error)

	s.opCh <- operation{
		type_:  SetPeerCurrentIndex,
		peerId: peer,
		index:  idx,
		errCh:  errCh,
	}

	<-errCh
}

// Append new entry to the log and return its index.
//
// If an entry conflicts with this one, existing entry and the ones after that are deleted.
func (s *State) AppendEntries(leaderCommitIndex int, idx int, entries ...store.LogEntry) error {
	errCh := make(chan error)

	s.opCh <- operation{
		type_:             AppendEntries,
		index:             idx,
		leaderCommitIndex: leaderCommitIndex,
		entries:           entries,
		errCh:             errCh,
	}

	return <-errCh
}
