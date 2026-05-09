package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/i-segura/toy-raft/raft/protocol"
)

type Client struct {
	address string
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) RequestVote(req protocol.RequestVoteRequest) (*protocol.RequestVoteResponse, error) {
	res := protocol.RequestVoteResponse{}
	return sendPost(c, &req, &res)
}

func (c *Client) AppendEntries(req protocol.AppendEntriesRequest) (*protocol.AppendEntriesResponse, error) {
	res := protocol.AppendEntriesResponse{}
	return sendPost(c, &req, &res)
}

type canSerialize interface {
	Serialize() ([]byte, error)
}

func sendPost[R any](c *Client, req canSerialize, res *R) (*R, error) {
	raw, err := req.Serialize()
	if err != nil {
		return nil, err
	}

	rx, err := http.NewRequest("POST", c.address, bytes.NewBuffer(raw))
	if err != nil {
		return nil, err
	}

	rx.Header.Add("Content-Type", "application/json")
	rx.Header.Add("Accept", "application/json")

	tx, err := http.DefaultClient.Do(rx)
	if err != nil {
		return nil, err
	}
	defer tx.Body.Close()
	if tx.StatusCode == 500 {
		return nil, fmt.Errorf("received status 500")
	}

	rawRes, err := io.ReadAll(tx.Body)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(rawRes, &res); err != nil {
		errMsg := protocol.Error{}
		err := json.Unmarshal(rawRes, &err)
		if err != nil {
			return nil, fmt.Errorf("unknown server response")
		}
		return nil, fmt.Errorf("protocol error: %s", errMsg.Cause)
	}

	return res, nil
}
