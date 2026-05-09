package raft

import (
	"encoding/json"
	"errors"
)

const RequestVoteMagicNumber = 0x01

type RequestVoteRequest struct {
}

type RequestVoteResponse struct{}

func (r *RequestVoteRequest) Serialize() ([]byte, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append([]byte{RequestVoteMagicNumber}, buf...), nil
}

func (r *RequestVoteRequest) Deserialize(data []byte) error {
	if len(data) < 1 {
		return errors.New("invalid request vote request")
	}
	if data[0] != RequestVoteMagicNumber {
		return errors.New("invalid request vote request")
	}
	return json.Unmarshal(data[1:], r)
}
