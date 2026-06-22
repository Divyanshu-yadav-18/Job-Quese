package api

import "net/http"

func NewRouter(h *Handler) http.Handler{
	mux := http.NewServeMux()

	mux.HandleFunc("POST /tasks", h.CreateTask)
	mux.HandleFunc("GET /tasks/{id}", h.GetTask)
	mux.HandleFunc("GET /queue/stats", h.GetStats)

	return mux
}
