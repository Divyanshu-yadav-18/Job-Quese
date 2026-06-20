package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/store"
	"github.com/redis/go-redis/v9"
)

type RedisDelayedQueue struct{
	client *redis.Client
	store *store.RedisStore
	key string
	notify chan struct{}
}

func NewRedisDelayedQueue(s *store.RedisStore, key string) *RedisDelayedQueue{
	return &RedisDelayedQueue{
		client: s.Client,
		store: s,
		key: key,
		notify: make(chan struct{},1),
	}
}

func (q *RedisDelayedQueue) Add(task *model.Task){
	ctx:= context.Background()

	if err := q.store.SaveTask(ctx, task); err != nil {
		fmt.Printf("[delayed] failed to save task %s: %v\n", task.ID, err)
		return
	}

	score := float64(task.RunAt.Unix())
	if err := q.client.ZAdd(ctx, q.key, redis.Z{
		Score: score,
		Member: task.ID,
	}).Err(); err != nil {
		fmt.Printf("[delayed] failed to zadd task %s: %v\n", task.ID, err)
		return
	}

	select{
	case q.notify <-struct{}{}:
	default:
	}
}

func (q *RedisDelayedQueue) PeekNext() *model.Task{
	ctx := context.Background()

	results, err := q.client.ZRangeWithScores(ctx, q.key, 0, 0).Result()
	if err != nil || len(results) == 0{
		return nil
	}

	taskID := results[0].Member.(string)
	task, err := q.store.GetTask(ctx, taskID)
	if err != nil{
		return nil 
	}
	return task
}

func (q *RedisDelayedQueue) PopReady() *model.Task{
	ctx := context.Background()
	now := float64(time.Now().Unix())

	pipe := q.client.TxPipeline()
	rangeCmd := pipe.ZRangeByScoreWithScores(ctx, q.key, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
		Offset: 0,
		Count: 1,
	})
	
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil{
		return nil
	}

	results, err := rangeCmd.Result()
	if err != nil || len(results) == 0 {
		return nil 
	}

	taskID := results[0].Member.(string)

	q.client.ZRem(ctx, q.key,taskID)

	task, err := q.store.GetTask(ctx, taskID)
	if err != nil{
		return nil
	}
	return task
} 

func (q *RedisDelayedQueue) Len() int {
    ctx := context.Background()
    n, err := q.client.ZCard(ctx, q.key).Result()
    if err != nil {
        return 0
    }
    return int(n)
}

func (q *RedisDelayedQueue) Notify() <-chan struct{} {
    return q.notify
}
