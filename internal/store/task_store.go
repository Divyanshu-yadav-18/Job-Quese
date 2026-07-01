package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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

// heartbeat

func (s *RedisStore) ClaimTask(ctx context.Context, workerID int, taskID string, ttl time.Duration) error {
	key := fmt.Sprintf("jq:worker:%d:state", workerID)
	state := map[string]interface{}{
		"status":    "running",
		"task_id":   taskID,
		"since":     time.Now().Unix(),
		"worker_id": workerID,
	}
	pipe := s.Client.TxPipeline()
	pipe.HSet(ctx, key, state)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)

	return err
}

func (s *RedisStore) HeartBeatWorker(ctx context.Context, workerID int, ttl time.Duration) error {
	key := fmt.Sprintf("jq:worker:%d:state", workerID)
	return s.Client.Expire(ctx, key, ttl).Err()
}

func (s *RedisStore) WorkerIdle(ctx context.Context, workerID int, ttl time.Duration) error {
	key := fmt.Sprintf("jq:worker:%d:state", workerID)
	state := map[string]interface{}{
		"status":    "idle",
		"task_id":   "",
		"since":     time.Now().Unix(),
		"worker_id": workerID,
	}
	pipe := s.Client.TxPipeline()
	pipe.HSet(ctx, key, state)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)

	return err
}

type WorkerState struct {
	WorkerID int
	Status   string
	TaskID   string
	Since    int64
}

func (s *RedisStore) AllWorkerStates(ctx context.Context, workerCount int) ([]WorkerState, error) {
	states := make([]WorkerState, 0)
	for i := 0; i < workerCount; i++ {
		key := fmt.Sprintf("jq:worker:%d:state", i)
		result, err := s.Client.HGetAll(ctx, key).Result()
		if err != nil || len(result) == 0 {
			states = append(states, WorkerState{WorkerID: i, Status: "dead"})
			continue
		}
		since, _ := strconv.ParseInt(result["since"], 10, 64)
		states = append(states, WorkerState{
			WorkerID: i,
			Status:   result["status"],
			TaskID:   result["task_id"],
			Since:    since,
		})
	}
	return states, nil
}

func (s *RedisStore) RegisterDependency(ctx context.Context, dependencyID, dependentID string) error {
	key := fmt.Sprintf("jq:task:dependents:%s", dependencyID)
	return s.Client.SAdd(ctx, key, dependentID).Err()
}

func (s *RedisStore) GetDependents(ctx context.Context, taskID string) ([]string, error) {
	key := fmt.Sprintf("jq:task:dependents:%s", taskID)
	return s.Client.SMembers(ctx, key).Result()

}

func (s *RedisStore) AreDependenciesMet (ctx context.Context, dependsOn []string) (bool, error) {
	for _, depID := range dependsOn{
		task, err := s.GetTask(ctx, depID)
		if err != nil{
			return false, fmt.Errorf("dependency %s not found: %w", depID, err)
		}
		if task.Status != model.StatusCompleted{
			return false, nil
		}
	}
	return true, nil
}

func (s *RedisStore) HasFailedDependency(ctx context.Context, dependsOn []string)(bool, string, error){
	for _, depId := range dependsOn{
		task, err := s.GetTask(ctx, depId)
		if err != nil {
			return false, "", err
		}
		if task.Status == model.StatusDead{
			return true, depId, nil
		}
	}

	return false, "", nil
}
