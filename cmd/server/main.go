package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/queue"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/worker"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		fmt.Println("\n[main] shutdown signal received")
		cancel()
	}()
	
	delayed := queue.NewDelayedQueue()
	ready := queue.New(100)
	pool := worker.NewPool(5, ready, delayed)
	scheduler := queue.NewScheduler(delayed, ready)

	immediateTasks := []struct {
		id       string
		priority int
	}{
		{"task-alpha", 9},
		{"task-beta", 3},
		{"task-gamma", 7},
		{"task-delta", 1},
		{"task-epsilon", 5},
	}

	for _, t := range immediateTasks {
		task := model.NewTask(t.id, "simulate", `{"msg":"hello"}`, t.priority)
		task.Status = model.StatusReady
		ready.Push(task)
		fmt.Printf("[main] pushed %s to ready queue (priority %d)\n", t.id, t.priority)

	}

	d1 := model.NewTask("task-delayed-5s", "simulate", `{}`, 5)
	d1.RunAt = time.Now().Add(5 * time.Second)
	delayed.Add(d1)
	fmt.Println("[main] pushed task-delayed-5s (runs in 5s)")

	d2 := model.NewTask("task-delayed-2s", "simulate", `{}`, 8)
	d2.RunAt = time.Now().Add(2 * time.Second)
	delayed.Add(d2)
	fmt.Println("[main] pushed task-delayed-2s (runs in 2s)")

	go scheduler.Run(ctx)

	pool.Start(ctx)
	fmt.Println("[main] clean exit")
}
