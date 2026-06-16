package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/queue"
)

type Pool struct {
	workers []*Worker
	size int
	ready *queue.Queue
	delayed *queue.DelayedQueue
}

func NewPool(size int, q *queue.Queue, d *queue.DelayedQueue) *Pool{
	workers := make([]*Worker, size)
	for i := range workers {
		workers[i] = New(i,q, d)
	}

	return &Pool{
		workers: workers,
		size: size,
		ready: q,
		delayed: d,
	}
}

func (p *Pool) Start (ctx context.Context) {
	var wg sync.WaitGroup

	for _, w := range p.workers {
		wg.Add(1)

		w := w
		go func(){
			defer wg.Done()
			w.Start(ctx)
		}()
	}
	fmt.Printf("[pool] %d workers running\n", p.size)
	wg.Wait()
	fmt.Println("[pool] all workers stopped")
}


