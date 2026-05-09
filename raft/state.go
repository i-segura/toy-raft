package raft

import (
	"time"

	"github.com/i-segura/toy-raft/raft/store"
)

type Role int

const Follower Role = 1
const Candidate Role = 2
const Leader Role = 3

type LogEntry struct {
	Term    int64
	Command interface{}
}

type State struct {
	role Role

	commitIndex int // Index of highest log entry known to be commited.
	lastApplied int // Index of highest log entry applied.

	leaderNextIndex  map[string]int // Index of the next log to send to each server.
	leaderMatchIndex map[string]int // Index of highest log entry known replicated on each server.

	store store.Store // Contains log and persistent state. Updated before responding to RPC.

	electionTimeout time.Timer
}
