package protocol

import (
	"encoding/json"
	"errors"
)

type RequestVoteRequest struct {
	Term         int    `json:"term"`           // Candidate's term.
	CandidateID  string `json:"candidate_id"`   // Candidate's requesting vote.
	LastLogIndex int    `json:"last_log_index"` // Index of candidate's last log entry.
	LastLogTerm  int    `json:"last_log_term"`  // Term of candidate's last log entry.
}

func (r *RequestVoteRequest) Serialize() ([]byte, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append([]byte{RequestVoteRequestMagicNumber}, buf...), nil
}

func (r *RequestVoteRequest) Deserialize(data []byte) error {
	if len(data) < 1 {
		return errors.New("invalid request vote request")
	}
	if data[0] != RequestVoteRequestMagicNumber {
		return errors.New("invalid request vote request")
	}
	return json.Unmarshal(data[1:], r)
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`         // Current term.
	VoteGranted bool `json:"vote_granted"` // True means received a vote.
}

func (r *RequestVoteResponse) Serialize() ([]byte, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append([]byte{RequestVoteResponseMagicNumber}, buf...), nil
}

func (r *RequestVoteResponse) Deserialize(data []byte) error {
	if len(data) < 1 {
		return errors.New("invalid request vote response")
	}
	if data[0] != RequestVoteResponseMagicNumber {
		return errors.New("invalid request vote response")
	}
	return json.Unmarshal(data[1:], r)
}
