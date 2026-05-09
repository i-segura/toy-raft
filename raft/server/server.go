package server

import (
	"bytes"
	"io"
	"log"
	"net/http"

	"github.com/i-segura/toy-raft/raft/protocol"
)

type Handler struct {
	OnRequestVote   func(protocol.RequestVoteRequest) (*protocol.RequestVoteResponse, *protocol.Error)
	OnAppendEntries func(protocol.AppendEntriesRequest) (*protocol.AppendEntriesResponse, *protocol.Error)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		if err := respondError(w, protocolError("invalid HTTP method"), 403); err != nil {
			log.Printf("error responding client: %s", err)
		}
	}

	magic := make([]byte, 1)
	_, err := r.Body.Read(magic)
	if err != nil {
		log.Printf("error reading request: %s", err)
	}

	if err = h.handleMessage(magic, r.Body, w); err != nil {
		log.Printf("error handling request: %s", err)
	}
}

func (h *Handler) handleMessage(magic []byte, r io.Reader, w http.ResponseWriter) error {
	switch magic[0] {
	case protocol.RequestVoteRequestMagicNumber:
		requestVote := protocol.RequestVoteRequest{}
		b, err := io.ReadAll(io.MultiReader(bytes.NewBuffer(magic), r))
		if err != nil {
			return respondError(w, protocolError("error reading message"), 400)
		}

		err = requestVote.Deserialize(b)
		if err != nil {
			return respondError(w, protocolError("malformed request vote request"), 400)
		}

		res, errCause := h.OnRequestVote(requestVote)
		if errCause != nil {
			return respondError(w, *errCause, 400)
		}
		return respondOk(w, res)
	case protocol.AppendEntriesRequestMagicNumber:
		appendEntry := protocol.AppendEntriesRequest{}
		b, err := io.ReadAll(io.MultiReader(bytes.NewBuffer(magic), r))
		if err != nil {
			return respondError(w, protocolError("error reading message"), 400)
		}

		err = appendEntry.Deserialize(b)
		if err != nil {
			return respondError(w, protocolError("malformed append entry request"), 400)
		}

		res, errCause := h.OnAppendEntries(appendEntry)
		if errCause != nil {
			return respondError(w, *errCause, 400)
		}
		return respondOk(w, res)
	default:
		return respondError(w, protocolError("unexpected message"), 403)
	}
}
