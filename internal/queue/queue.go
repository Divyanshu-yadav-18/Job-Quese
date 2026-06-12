package queue

import (
	"errors"
	"sync"

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
			q.tasks[i] = inserted
			inserted = true
			break
		} 
	}

	if !inserted{
		q.tasks = append(q.tasks, task)
	}

	select {
	case q.notify <- struct{}{}:
		default:
	}
	return nil
}
