package queue

import (
	"context"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
)

type JobQueue interface{
	Push(task *model.Task) error
	Pop()(*model.Task, error)
	Wait (ctx context.Context)
	Len() int
	Peek(n int) []*model.Task
}

type DelayedJobQueue interface{
	Add(task *model.Task)
	PeekNext() *model.Task
	PopReady() *model.Task
	Len() int
	Notify() <-chan struct{}
}
