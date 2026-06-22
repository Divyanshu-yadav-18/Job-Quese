package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/api"
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
	handler := api.NewHandler(ready, delayed, redisStore)
	router := api.NewRouter(handler)
	server := &http.Server{Addr: ":8080", Handler: router}

	go func ()  {
		fmt.Println("[http] listening on: 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("[http] error", err)
		}
	}()

	go scheduler.Run(ctx)
	go pool.Start(ctx)

	<-ctx.Done()
	server.Close()
	fmt.Println("[main] clean exit")
}
