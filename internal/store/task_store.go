package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
	"github.com/redis/go-redis/v9"
)

func (s *RedisStore) SaveTask(ctx context.Context, task *model.Task) error {
	data, err := json.Marshal(task)

	if err != nil {
		return fmt.Errorf("Marshal task %s: %w", task.ID, err)
	}
	return s.Client.Set(ctx, TaskKey(task.ID), data, 24*time.Hour).Err()
}

func (s *RedisStore) GetTask(ctx context.Context, id string) (*model.Task, error) {
	data, err := s.Client.Get(ctx, TaskKey(id)).Bytes()

	if err == redis.Nil {
		return nil, fmt.Errorf("task %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get task %s: %w", id, err)
	}

	var task model.Task

	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("Unmarshal task %s: %w", id, err)
	}

	return &task, nil

}
