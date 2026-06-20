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
	queue queue.JobQueue
}

func NewPool(size int, q queue.JobQueue, dq queue.DelayedJobQueue) *Pool{
	workers := make([]*Worker, size)
	for i := range workers {
		workers[i] = New(i,q, dq)
	}

	return &Pool{
		workers: workers,
		size: size,
		queue: q,
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


