package queue

import (
	"container/heap"
	"sync"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
)

type delayedItem struct {
	task *model.Task
}

type delayedHeap []*delayedItem

func (h delayedHeap) Len() int { return len(h) }

func (h delayedHeap) Less(i, j int) bool {
	return h[i].task.RunAt.Before(h[j].task.RunAt)
}

func (h delayedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *delayedHeap) Push(x any) {
	item := x.(*delayedItem)
	*h = append(*h, item)
}

func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// DelayedQueue holds tasks scheuled to run in the future

type DelayedQueue struct {
	mu     sync.Mutex
	h      delayedHeap
	notify chan struct{}
}

func NewDelayedQueue() *DelayedQueue {
	dq := &DelayedQueue{h: delayedHeap{}, notify: make(chan struct{}, 1)}
	heap.Init(&dq.h)
	return dq
}

func (dq *DelayedQueue) Add(task *model.Task) {
	dq.mu.Lock()
	heap.Push(&dq.h, &delayedItem{task: task})

	dq.mu.Unlock()

	select {
	case dq.notify <- struct{}{}:
	default:
	}
}
func (dq *DelayedQueue) Notify() <-chan struct{}{
	return dq.notify
}

func (dq *DelayedQueue) PeekNext() *model.Task {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	if len(dq.h) == 0 {
		return nil
	}
	return dq.h[0].task
}

func (dq *DelayedQueue) PopReady() *model.Task {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	if len(dq.h) == 0 {
		return nil
	}

	if dq.h[0].task.IsDelayed() {
		return nil
	}
	item := heap.Pop(&dq.h).(*delayedItem)
	return item.task
}

func (dq *DelayedQueue) Len() int {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	return len(dq.h)
}
