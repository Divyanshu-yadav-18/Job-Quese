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

	q := queue.New(100)
	pool := worker.NewPool(5, q)

	tasks := []struct {
		id       string
		priority int
	}{
		{"task-alpha", 9},
		{"task-beta", 3},
		{"task-gamma", 7},
		{"task-delta", 1},
		{"task-epsilon", 5},
	}

	for _, t := range tasks {
		task := model.NewTask(t.id, "simulate", `{"msg":"hello"}`, t.priority)
		q.Push(task)
		fmt.Printf("[main] pushed %s (priority %d)\n", t.id, t.priority)

	}

	delayed := model.NewTask("task-delayed", "simulate", `{}`, 5)
	delayed.RunAt = time.Now().Add(5 * time.Second)
	q.Push(delayed)
	fmt.Println("[main] pushed delayed task (runs in 5s)")

	pool.Start(ctx)
	fmt.Println("[main] clean exit")
}
