package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
)

type Scheduler struct {
	delayed *DelayedQueue
	ready   *Queue
}

func NewScheduler(delayed *DelayedQueue, ready *Queue) *Scheduler {
	return &Scheduler{delayed: delayed, ready: ready}
}

func (s *Scheduler) Run(ctx context.Context) {
	fmt.Println("[scheduler] started")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("[scheduler] shutting down")
			return
		default:
		}

		next := s.delayed.PeekNext()

		if next == nil {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return
			}
			continue
		}

		wait := time.Until(next.RunAt)

		if wait <= 0 {
			task := s.delayed.PopReady()

			if task != nil {
				task.Status = model.StatusReady
				s.ready.Push(task)
				fmt.Printf("[scheduler] promoted %s to ready queue\n", task.ID)
			}
			continue
		}
		select {
		case <-time.After(wait):
		case <-s.delayed.Notify():
		case <-ctx.Done():
			return
		}
	}

}
