package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/i-segura/toy-raft/raft/protocol"
)

type canSerialize interface {
	Serialize() ([]byte, error)
}

func respondOk(w http.ResponseWriter, s canSerialize) error {
	return respondSerializeJson(w, s, 200)
}

func respondError(w http.ResponseWriter, protoErr protocol.Error, code int) error {
	return respondSerializeJson(w, &protoErr, code)
}

func respondSerializeJson(w http.ResponseWriter, s canSerialize, code int) error {
	raw, err := s.Serialize()
	if err != nil {
		w.WriteHeader(500)
		return nil
	}

	w.Header().Add("Content-Type", "application/json")
	if _, err = io.Copy(w, bytes.NewBuffer(raw)); err != nil {
		w.WriteHeader(500)
		return fmt.Errorf("error writing response: %s", err)
	}
	return nil
}

func protocolError(cause string) protocol.Error {
	return protocol.Error{
		Cause: cause,
	}
}
