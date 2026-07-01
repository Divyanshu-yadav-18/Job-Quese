package worker

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/queue"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/store"
)

// worker -> ID, Queue, Status
type Worker struct {
	ID      int
	ready   queue.JobQueue
	delayed queue.DelayedJobQueue
	store   *store.RedisStore
	status  string
}

func New(id int, ready queue.JobQueue, delayed queue.DelayedJobQueue, s *store.RedisStore) *Worker {
	return &Worker{
		ID:      id,
		ready:   ready,
		delayed: delayed,
		store:   s,
		status:  "idle",
	}
}

//It shouldn't stop until context is cancelled, a running loop

func (w *Worker) Start(ctx context.Context) {
	fmt.Printf("[W%d] started\n", w.ID)

	existing, err := w.store.AllWorkerStates(ctx, w.ID+1)
	if err == nil && len(existing) > w.ID {
		prev := existing[w.ID]
		if prev.Status == "running" && prev.TaskID != "" {
			fmt.Printf("[W%d] found orphaned task %s from previous run\n",
				w.ID, prev.TaskID)
		}

	}

	w.store.WorkerIdle(ctx, w.ID, 30*time.Second)

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
			w.store.WorkerIdle(ctx, w.ID, 30*time.Second)
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

	w.store.ClaimTask(ctx, w.ID, task.ID, 10*time.Second)
	w.store.SaveTask(ctx, task)

	fmt.Printf("[W%d] picked up task %s (attempt %d/%d)\n", w.ID, task.ID, task.Attempts, task.MaxRetries)

	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.store.HeartBeatWorker(ctx, w.ID, 10*time.Second)
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	err := simulateWork(ctx, task)
	close(heartbeatDone)
	if err == nil {
		done := time.Now()
		task.CompletedAt = &done
		task.Status = model.StatusCompleted
		w.store.SaveTask(ctx, task)
		fmt.Printf("[W%d] completed task %s in %d ms }\n", w.ID, task.ID, time.Since(now).Milliseconds())

		w.resolveDependents(ctx, task)

		return
	}

	task.LastError = err.Error()
	fmt.Printf("[W%d] task %s failed: %s\n", w.ID, task.ID, err)

	if task.CanRetry() {
		backoff := task.NextBackoff()
		task.RunAt = time.Now().Add(backoff)
		task.Status = model.StatusPending
		w.store.SaveTask(ctx, task)
		fmt.Printf("[W%d] retrying task %s in %s \n", w.ID, task.ID, backoff)
		w.delayed.Add(task)
	} else {
		task.Status = model.StatusDead
		w.store.SaveTask(ctx, task)
		fmt.Printf("[W%d] task %s exhausted retries -> dead letter\n", w.ID, task.ID)
		w.resolveDependents(ctx, task)

		//Dead letter Queue logic
	}
}

func simulateWork(ctx context.Context, task *model.Task) error {
	duration := time.Duration(200+rand.Intn(600)) * time.Millisecond

	if task.Type == "slow" {
		duration = 15 * time.Second
	}

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

func (w *Worker) resolveDependents(ctx context.Context, completedTask *model.Task) {
	dependents, err := w.store.GetDependents(ctx, completedTask.ID)
	if err != nil || len(dependents) == 0 {
		return
	}

	for _, depID := range dependents {
		dependent, err := w.store.GetTask(ctx, depID)
		if err != nil {
			fmt.Printf("[W%d] could not fetch dependent %s: %v\n", w.ID, depID, err)
			continue
		}

		if dependent.Status != model.StatusBlocked {
			continue
		}

		failed, failedDepID, err := w.store.HasFailedDependency(ctx, dependent.DependsOn)
		if err != nil {
			continue
		}

		if failed {
			dependent.Status = model.StatusDead
			dependent.LastError = fmt.Sprintf("dependency %s failed", failedDepID)
			w.store.SaveTask(ctx, dependent)
			fmt.Printf("[W%d] task %s failed because dependency %s died\n",
				w.ID, depID, failedDepID)
			continue
		}

		met, err := w.store.AreDependenciesMet(ctx, dependent.DependsOn)
		if err != nil || !met {
			continue
		}

		dependent.Status = model.StatusReady
		w.store.SaveTask(ctx, dependent)
		w.ready.Push(dependent)
		fmt.Printf("[W%d] unblocked task %s → ready queue\n", w.ID, depID)
	}
}
