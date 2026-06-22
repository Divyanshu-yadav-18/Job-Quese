package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/queue"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/store"
	"github.com/google/uuid"
)

type Handler struct {
	ready   queue.JobQueue
	delayed queue.DelayedJobQueue
	store   *store.RedisStore
}

func NewHandler(ready queue.JobQueue, delayed queue.DelayedJobQueue, s *store.RedisStore) *Handler {
	return &Handler{ready: ready, delayed: delayed, store: s}
}

func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" {
		respondError(w, http.StatusBadRequest, "type is required")
		return
	}

	id := req.ID
	if id == "" {
		id = uuid.New().String()
	} else {
		existing, err := h.store.GetTask(r.Context(), id)
		if err == nil && existing != nil {
			respondJSON(w, http.StatusOK, toTaskResponse(existing))
			return
		}
	}

	task := model.NewTask(id, req.Type, req.Payload, req.Priority)
	task.DependsOn = req.DependsOn

	if req.MaxRetries > 0 {
		task.MaxRetries = req.MaxRetries
	}

	if req.DelaySecs > 0 {
		task.RunAt = time.Now().Add(time.Duration(req.DelaySecs) * time.Second)
		task.Status = model.StatusPending
		h.delayed.Add(task)
	} else {
		task.Status = model.StatusReady
		if err := h.ready.Push(task); err != nil {
			respondError(w, http.StatusInternalServerError, "failed to Enqueue task")
			return
		}
	}
	respondJSON(w, http.StatusCreated, toTaskResponse(task))
}

func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	task, err := h.store.GetTask(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, fmt.Sprintf("task %s not found", id))
		return
	}
	respondJSON(w, http.StatusOK, toTaskResponse(task))
}
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	stats := StatsResponse{
		ReadyCount:   h.ready.Len(),
		DelayedCount: h.delayed.Len(),
	}
	respondJSON(w, http.StatusOK, stats)
}

func toTaskResponse(t *model.Task) TaskResponse {
	return TaskResponse{
		ID:         t.ID,
		Type:       t.Type,
		Status:     string(t.Status),
		Priority:   t.Priority,
		Attempts:   t.Attempts,
		MaxRetries: t.MaxRetries,
		LastError:  t.LastError,
		CreatedAt:  t.CreatedAt.Format(time.RFC3339),
	}
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
