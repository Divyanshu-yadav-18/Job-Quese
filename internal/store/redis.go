package store

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	KeyReadyQueue = "jq:queue:ready"
	KeyDelayedQueue = "jq:queue:delayed"
	KeyDeadLetter = "jq:deadletter"
	KeyMatrics = "jq:matrics"
	KeyEvents = "jq:events"

	KeyTaskPrefix = "jq:task:"
	KeyWorkerPrefix = "jq:worker:"

	KeyWorkerStatus = "jq:workers"
)

func TaskKey (id string) string {
	 return  KeyTaskPrefix + id
}

func WorkerKey(id string) string {
	return  KeyWorkerPrefix+ id + ":state"
}

type RedisStore struct {
	Client *redis.Client
}

func NewRedisStore(addr string) (*RedisStore, error){
	client :=  redis.NewClient(&redis.Options{
		Addr: addr,
		Password: "",
		DB: 0,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connect failed: %w", err)
	}
	fmt.Println("[store] connected to Redis at", addr)
	return &RedisStore{Client: client}, nil
}

func (s *RedisStore) Close() error{
	return s.Client.Close()
}
