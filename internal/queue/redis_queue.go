package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
	"github.com/Divyanshu-yadav-18/Job-Quese/internal/store"
	"github.com/redis/go-redis/v9"
)

type RedisQueue struct{
	client *redis.Client
	store *store.RedisStore
	key string
	notify chan struct{}
}

func NewRedisQueue(s *store.RedisStore, key string) *RedisQueue{
	return &RedisQueue{
		client: s.Client,
		store: s,
		key: key,
		notify: make(chan struct{},1),
	}
}

func (q *RedisQueue) Push (task *model.Task) error{
	ctx := context.Background()

	if err := q.store.SaveTask(ctx, task); err != nil {
		return fmt.Errorf("push save task: %w", err)
	}

	score := float64(-task.Priority)
	if err := q.client.ZAdd(ctx, q.key, redis.Z{
		Score: score,
		Member: task.ID,
	}).Err(); err != nil{
		return fmt.Errorf("push Zadd: %w", err)
	}

select{
	case q.notify <-struct{}{}:
	default:
	}

	return nil
}

func (q *RedisQueue) Pop() (*model.Task, error){
	ctx := context.Background()

	results,err := q.client.ZPopMin(ctx, q.key, 1).Result()
	if err != nil{
		return nil, fmt.Errorf("pop zpopmin: %w", err)
	}

	if len(results) == 0{
		return  nil, ErrQueueEmpty
	}

	taskID := results[0].Member.(string)


	task, err := q.store.GetTask(ctx, taskID)
		if err != nil{
			return nil, fmt.Errorf("pop get task %s: %w",taskID, err)
		
	}
	return task, nil
} 

func (q *RedisQueue) Wait(ctx context.Context){
	select{
	case <-q.notify:
	case <-ctx.Done():
	case <-time.After(500*time.Millisecond):
	}
}

func (q *RedisQueue) Len() int{
	ctx := context.Background()
	n, err := q.client.ZCard(ctx, q.key).Result()
	if err != nil{
		return 0
	}
	return int(n)
}

func (q *RedisQueue) Peek(n int) []*model.Task{
	ctx := context.Background()

	results, err := q.client.ZRange(ctx, q.key, 0, int64(n-1)).Result()
	if err != nil || len(results) == 0{
		return nil
	}

	tasks := make([]*model.Task, 0, len(results))


	for _, id := range results{
		task, err:=q.store.GetTask(ctx,id)
		if err != nil{
			continue
		}
		tasks = append(tasks, task)

	}
	return tasks
}
