package worker

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/queue"
)

// worker -> ID, Queue, Status
type Worker struct {
	ID      int
	ready   queue.JobQueue
	delayed queue.DelayedJobQueue
	status  string
}

func New(id int, ready queue.JobQueue, delayed queue.DelayedJobQueue) *Worker {
	return &Worker{
		ID:     id,
		ready: ready,
		delayed: delayed,
		status: "idle",
	}
}

//It shouldn't run until context is cancelled, a running loop

func (w *Worker) Start(ctx context.Context) {
	fmt.Printf("[W%d] started\n", w.ID)

	for {
		//check context done
		select {
		case <-ctx.Done():
			fmt.Printf("[W%d] shutting down\n", w.ID)
			return
		default:
			// context is alive
		}

		task, err := w.ready.Pop()
		if err == queue.ErrQueueEmpty {
			w.status = "idle"
			w.ready.Wait(ctx)
			continue
		}

		w.status = "running"
		w.execute(ctx, task)
	}
}

// execute runs a single task
func (w *Worker) execute(ctx context.Context, task *model.Task) {
	now := time.Now()
	task.StartedAt = &now
	task.Status = model.StatusRunning
	task.Attempts++

	fmt.Printf("[W%d] picked up task %s (attempt %d/%d)\n", w.ID, task.ID, task.Attempts, task.MaxRetries)

	err := simulateWork(ctx, task)

	if err == nil {
		done := time.Now()
		task.CompletedAt = &done
		task.Status = model.StatusCompleted

		fmt.Printf("[W%d] completed task %s in %d ms }\n", w.ID, task.ID, time.Since(now).Milliseconds())

		return
	}

	task.LastError = err.Error()
	fmt.Printf("[W%d] task %s failed: %s\n", w.ID, task.ID, err)

	if task.CanRetry() {
		backoff := task.NextBackoff()
		task.RunAt = time.Now().Add(backoff)
		task.Status = model.StatusPending

		fmt.Printf("[W%d] retrying task %s in %s \n", w.ID, task.ID, backoff)
		w.delayed.Add(task)
	} else {
		task.Status = model.StatusDead
		fmt.Printf("[W%d] task %s exhausted retries -> dead letter\n", w.ID, task.ID)

		//Dead letter Queue logic
	}
}

func simulateWork(ctx context.Context, task *model.Task) error {
	duration := time.Duration(200+rand.Intn(600)) * time.Millisecond

	select {
	case <-time.After(duration):
	case <-ctx.Done():
		return fmt.Errorf("Work Cancelled ")
	}

	if rand.Float32() < 0.30 {
		return fmt.Errorf("Simulated Failure for task %s", task.ID)
	}
	return nil
}
