package state

import (
	"github.com/i-segura/toy-raft/raft/store"
)

type Role int

const Follower Role = 1
const Candidate Role = 2
const Leader Role = 3

type State struct {
	role Role

	currentLeader string // Current leader.
	commitIndex   int    // Index of highest log entry known to be commited.
	lastApplied   int    // Index of highest log entry applied.

	leaderNextIndex  map[string]int // Index of the next log to send to each server.
	leaderMatchIndex map[string]int // Index of highest log entry known replicated on each server.

	store store.Store // Contains log and persistent state. Updated before responding to RPC.

}

func (s *State) SetCurrentLeader(leader string) {
	s.currentLeader = leader
}

func (s *State) SetRole(role Role) {
	s.role = role
}

// Latest term server has seen.
func (s *State) CurrentTerm() int {
	return s.store.Snapshot().CurrentTerm
}

// Set term.
func (s *State) SetTerm(newTerm int) error {
	if newTerm <= s.store.Snapshot().CurrentTerm {
		return nil
	}

	return s.store.WriteTerm(newTerm)
}

// Who received vote in current term. Empty if none.
func (s *State) VotedFor() string {
	return s.store.Snapshot().VotedFor
}

// Updated voted for candidate this term. Reset election timeout.
func (s *State) VoteFor(candidate string) error {
	if candidate == s.store.Snapshot().VotedFor {
		return nil
	}

	return s.store.WriteVotedFor(candidate)
}

// Index of highest log entry known to be commited.
func (s *State) CommitIndex() int {
	return s.commitIndex
}

// Update last commit index.
func (s *State) SetCommitIndex(idx int) {
	s.commitIndex = idx
}

// Append new entry to the log and return its index.
//
// If an entry conflicts with this one, existing entry and the ones after that are deleted.
func (s *State) AppendEntries(idx int, entries ...store.LogEntry) (int, error) {
	err := s.store.WriteEntries(idx, entries...)
	if err != nil {
		return 0, err
	}

	return idx + len(entries), nil
}

// Get log entry's term. -1 if not found.
func (s *State) EntryTerm(idx int) int {
	snap := s.store.Snapshot()

	if len(snap.Log) <= idx {
		return -1
	}

	return snap.Log[idx].Term
}
