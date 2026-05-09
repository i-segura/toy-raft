package protocol

import (
	"encoding/json"
	"errors"
)

type AppendEntriesRequest struct {
	Term               int    `json:"term"`                  // Leader's term.
	LeaderID           string `json:"leader_id"`             // To redirect clients.
	PrevLogIndex       int    `json:"prev_log_index"`        // Index of log entry immediately preceding new ones.
	PrevLogTerm        int    `json:"prev_log_term"`         // Term of PrevLogIndex entry.
	PrevLogTermEntries []any  `json:"prev_log_term_entries"` // Entries to store. Empty means heartbeat.
	LeaderCommit       int    `json:"leader_commit"`         // Leader's commit index.
}

func (r *AppendEntriesRequest) Serialize() ([]byte, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append([]byte{AppendEntriesRequestMagicNumber}, buf...), nil
}

func (r *AppendEntriesRequest) Deserialize(data []byte) error {
	if len(data) < 1 {
		return errors.New("invalid append entries request")
	}
	if data[0] != AppendEntriesRequestMagicNumber {
		return errors.New("invalid append entries request")
	}
	return json.Unmarshal(data[1:], r)
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`    // Current term, for leader to update itself.
	Success bool `json:"success"` // True if follower contained entry matching PrevLogIndex and PrevLogTerm.
}

func (r *AppendEntriesResponse) Serialize() ([]byte, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append([]byte{AppendEntriesResponseMagicNumber}, buf...), nil
}

func (r *AppendEntriesResponse) Deserialize(data []byte) error {
	if len(data) < 1 {
		return errors.New("invalid append entries response")
	}
	if data[0] != AppendEntriesResponseMagicNumber {
		return errors.New("invalid append entries response")
	}
	return json.Unmarshal(data[1:], r)
}
