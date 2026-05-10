package data

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/i-segura/toy-raft/raft"
	"github.com/i-segura/toy-raft/raft/state"
)

// Handler serves POST / with a JSON object body. Leaders replicate via Raft; followers proxy to
// the leader's HTTP base URL from DataURLs (peer id -> "http://host:port", no trailing slash).
type Handler struct {
	Node     *raft.Node
	State    *state.State
	DataURLs map[string]string
}

func NewHandler(node *raft.Node, st *state.State, dataURLs map[string]string) *Handler {
	return &Handler{
		Node:     node,
		State:    st,
		DataURLs: dataURLs,
	}
}

const proposeTimeout = 15 * time.Second

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body"})
		return
	}

	var cmd map[string]any
	if err := json.Unmarshal(body, &cmd); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	snap := h.State.Snapshot()
	switch snap.Role {
	case state.Leader:
		ctx, cancel := context.WithTimeout(r.Context(), proposeTimeout)
		defer cancel()
		idx, err := h.Node.Propose(ctx, cmd)
		if err != nil {
			if err == raft.ErrNotLeader {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": idx})
	case state.Follower, state.Candidate:
		if snap.CurrentLeader == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader yet"})
			return
		}
		base, ok := h.DataURLs[snap.CurrentLeader]
		if !ok {
			log.Printf("data: no HTTP URL for leader %q", snap.CurrentLeader)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "leader URL unknown"})
			return
		}
		target := strings.TrimRight(base, "/") + "/"
		reqOut, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "proxy request"})
			return
		}
		reqOut.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: proposeTimeout}
		resp, err := client.Do(reqOut)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "read leader response"})
			return
		}
		for k, vv := range resp.Header {
			if k == "Content-Length" {
				continue
			}
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unknown role"})
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
