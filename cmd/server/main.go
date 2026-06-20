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
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/store"
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

	redisStore, err := store.NewRedisStore("localhost:6379")
	if err != nil {
		fmt.Println("failed to connect to redis:", err)
		os.Exit(1)
	}
	defer redisStore.Close()

	ready := queue.NewRedisQueue(redisStore, store.KeyReadyQueue)
	delayed := queue.NewRedisDelayedQueue(redisStore, store.KeyDelayedQueue)
	scheduler := queue.NewScheduler(delayed, ready)
	pool := worker.NewPool(5, ready, delayed)

	// Push test tasks
	for _, t := range []struct {
		id       string
		priority int
	}{
		{"task-alpha", 9}, {"task-beta", 3}, {"task-gamma", 7},
		{"task-delta", 1}, {"task-epsilon", 5},
	} {
		task := model.NewTask(t.id, "simulate", `{}`, t.priority)
		task.Status = model.StatusReady
		ready.Push(task)
		fmt.Printf("[main] pushed %s (priority %d)\n", t.id, t.priority)
	}

	d1 := model.NewTask("task-delayed-5s", "simulate", `{}`, 5)
	d1.RunAt = time.Now().Add(5 * time.Second)
	delayed.Add(d1)

	go scheduler.Run(ctx)
	pool.Start(ctx)
	fmt.Println("[main] clean exit")
}
