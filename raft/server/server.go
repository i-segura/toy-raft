package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/i-segura/toy-raft/raft/protocol"
)

type Handler struct {
	onRequestVote   func(protocol.RequestVoteRequest) (*protocol.RequestVoteResponse, *protocol.Error)
	onAppendEntries func(protocol.AppendEntriesRequest) (*protocol.AppendEntriesResponse, *protocol.Error)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		if err := respondError(w, protocolError("invalid HTTP method"), 403); err != nil {
			log.Printf("error responding client: %w", err)
		}
	}

	magic := make([]byte, 1)
	_, err := r.Body.Read(magic)
	if err != nil {
		log.Printf("error reading request: %w", err)
	}

	if err = h.handleMessage(magic, r.Body, w); err != nil {
		log.Printf("error handling request: %w", err)
	}
}

func (h *Handler) handleMessage(magic []byte, r io.Reader, w http.ResponseWriter) error {
	switch magic[0] {
	case protocol.RequestVoteRequestMagicNumber:
		requestVote := protocol.RequestVoteRequest{}
		err := json.NewDecoder(io.MultiReader(bytes.NewBuffer(magic), r)).Decode(&requestVote)
		if err != nil {
			return respondError(w, protocolError("malformed request vote request"), 400)
		}
		res, errCause := h.onRequestVote(requestVote)
		if errCause != nil {
			return respondError(w, *errCause, 400)
		}
		return respondOk(w, res)
	case protocol.AppendEntriesRequestMagicNumber:
		appendEntry := protocol.AppendEntriesRequest{}
		err := json.NewDecoder(io.MultiReader(bytes.NewBuffer(magic), r)).Decode(&appendEntry)
		if err != nil {
			return respondError(w, protocolError("malformed append entry request"), 400)
		}
		res, errCause := h.onAppendEntries(appendEntry)
		if errCause != nil {
			return respondError(w, *errCause, 400)
		}
		return respondOk(w, res)
	default:
		return respondError(w, protocolError("unexpected message"), 403)
	}
}
