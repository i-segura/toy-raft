package state

import "github.com/i-segura/toy-raft/raft/store"

type operationType int

const (
	BecomeFollower operationType = iota
	BecomeCandidate
	BecomeLeader
	UpdateLeader
	CastVote
	SetPeerNextIndex
	SetPeerCurrentIndex
	AppendEntries
)

type operation struct {
	type_ operationType

	term   int
	peerId string
	index  int

	leaderCommitIndex int
	entries           []store.LogEntry

	errCh chan<- error
}

func (s *State) handleOperation(op operation) {
	switch op.type_ {
	case BecomeFollower:
		s.becomeFollowerOperation(op)
	case BecomeCandidate:
		s.becomeCandidateOperation(op)
	case BecomeLeader:
		s.becomeLeaderOperation(op)
	case UpdateLeader:
		s.updateLeader(op)
	case CastVote:
		s.castVoteOperation(op)
	case SetPeerNextIndex:
		s.setPeerNextIndexOperation(op)
	case SetPeerCurrentIndex:
		s.setPeerCurrentIndexOperation(op)
	case AppendEntries:
		s.appendEntriesOperation(op)
	}
}

func (s *State) becomeFollowerOperation(op operation) {
	s.currentLeader = op.peerId
	s.role = Follower
	s.leaderMatchIndex = map[string]int{}
	s.leaderNextIndex = map[string]int{}
	op.errCh <- s.store.WriteTerm(op.term)
}

func (s *State) becomeCandidateOperation(op operation) {
	s.role = Candidate

	op.errCh <- s.store.WriteVotedFor(s.store.Data.CurrentTerm+1, op.peerId)
}

func (s *State) becomeLeaderOperation(op operation) {
	s.currentLeader = op.peerId
	s.role = Leader

	op.errCh <- nil
}

func (s *State) updateLeader(op operation) {
	s.currentLeader = op.peerId

	op.errCh <- nil
}

func (s *State) castVoteOperation(op operation) {
	op.errCh <- s.store.WriteVotedFor(op.term, op.peerId)
}

func (s *State) setPeerNextIndexOperation(op operation) {
	s.leaderNextIndex[op.peerId] = op.index
	op.errCh <- nil
}

func (s *State) setPeerCurrentIndexOperation(op operation) {
	s.leaderMatchIndex[op.peerId] = op.index
	op.errCh <- nil
}

func (s *State) appendEntriesOperation(op operation) {
	err := s.store.WriteEntries(op.index, op.entries...)
	if err != nil {
		op.errCh <- err
		return
	}

	lastIndex := op.index + len(op.entries)
	if op.leaderCommitIndex > s.commitIndex {
		s.commitIndex = min(op.leaderCommitIndex, lastIndex)
	}

	op.errCh <- nil
}
