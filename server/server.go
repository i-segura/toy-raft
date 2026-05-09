package server

import (
	"context"
	"net/http"
)

type Server struct {
	httpServer *http.Server
}

func New(addr string) *Server {

	s := &Server{
		httpServer: &http.Server{
			Addr: addr,
		},
	}
	s.httpServer.Handler = &Handler{}
	return s
}

func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Stop() error {
	return s.httpServer.Shutdown(context.Background())
}

type Handler struct {
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Hello, World!"))
}
