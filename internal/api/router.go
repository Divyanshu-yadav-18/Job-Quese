package api

import "net/http"

func NewRouter(h *Handler) http.Handler{
	mux := http.NewServeMux()

	mux.HandleFunc("POST /task", h.CreateTask)
	mux.HandleFunc("GET /task{id}", h.GetTask)
	mux.HandleFunc("GET /queue/stats", h.GetStats)

	return mux
}
