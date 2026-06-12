package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
)

//Resposibilities
// hold tasks waiting
// let multiple worker grab tasks (concurrency safety)

// standard errors
var (
	ErrQueueFull  = errors.New("queue is full")
	ErrQueueEmpty = errors.New("queue is empty")
)

type Queue struct {
	tasks  []*model.Task
	mu     sync.Mutex
	cap    int
	notify chan struct{}
}

func New(capacity int) *Queue {
	return &Queue{
		tasks:  make([]*model.Task, 0),
		cap:    capacity,
		notify: make(chan struct{}, 1),
	}
}

// adding tasks in queue

func (q *Queue) Push(task *model.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.cap > 0 && len(q.tasks) >= q.cap {
		return ErrQueueFull
	}

	inserted := false
	for i, t := range q.tasks {
		if task.Priority > t.Priority {

			q.tasks = append(q.tasks, nil)
			copy(q.tasks[i+1:], q.tasks[i:])
			q.tasks[i] = task
			inserted = true
			break
		}
	}

	if !inserted {
		q.tasks = append(q.tasks, task)
	}

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return nil
}

func (q *Queue) Pop() (*model.Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	for i, task := range q.tasks {
		if task.RunAt.After(now) {
			continue
		}

		q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)

		return task, nil
	}

	return nil, ErrQueueEmpty
}

func (q *Queue) Wait(ctx context.Context) {
	select {
	case <-q.notify:
	case <-ctx.Done():
	case <-time.After(500 * time.Millisecond):
	}
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *Queue) Peek(n int) []*model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if n > len(q.tasks) {
		n = len(q.tasks)
	}

	result := make([]*model.Task, n)
	copy(result, q.tasks[:n])
	return result
}
