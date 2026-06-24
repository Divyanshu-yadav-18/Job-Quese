package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/queue"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/store"
)

type Monitor struct {
	store       *store.RedisStore
	delayed     queue.DelayedJobQueue
	workerCount int
}

func NewMonitor(s *store.RedisStore, delayed queue.DelayedJobQueue, workerCount int) *Monitor {
	return &Monitor{
		store:       s,
		delayed:     delayed,
		workerCount: workerCount,
	}
}

func (m *Monitor) Run(ctx context.Context) {
	fmt.Println("[monitor] started")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("[monitor] shutting down")
			return
		case <-ticker.C:
			m.checkWorker(ctx)
		}
	}
}

func (m *Monitor) checkWorker(ctx context.Context) {
	states, err := m.store.AllWorkerStates(ctx, m.workerCount)
	if err != nil {
		fmt.Println("[monitor] error fetching worker states:", err)
		return
	}

	for _, state := range states {
		if state.Status != "dead" {
			continue
		}

		if state.TaskID == "" {
			continue
		}

		fmt.Printf("[monitor] W%d is dead, recovering task %s\n",
			state.WorkerID, state.TaskID)

		task, err := m.store.GetTask(ctx, state.TaskID)
		if err != nil {
			fmt.Printf("[monitor] could not fetch task %s: %v\n", state.TaskID, err)
			continue
		}
		if task.Status == "completed" || task.Status == "dead" {
			continue
		}

		task.RunAt = time.Now().Add(5 * time.Second)
		m.delayed.Add(task)
		fmt.Printf("[monitor] requeued task %s → delayed queue\n", task.ID)

	}
}
