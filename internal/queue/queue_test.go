package queue

import (
	"testing"

	"github.com/Divyanshu-yadav-18/Job-Quese/internal/model"
)

func TestPushPopPriority(t *testing.T) {
	q := New(10)

	q.Push(model.NewTask("low", "email", "{}", 1))
	q.Push(model.NewTask("mid", "email", "{}", 5))
	q.Push(model.NewTask("high", "email", "{}", 9))

	task, err := q.Pop()

	if err != nil {
		t.Fatalf("expected task, got error %v", err)
	}

	if task.ID != "high" {
		t.Errorf("expected 'high', got %s", task.ID)
	}

	task, _ = q.Pop()
	if task.ID != "mid" {
		t.Errorf("expected 'mid', got %s", task.ID)

	}

}

func TestQueueFull(t *testing.T) {
	q := New(2)

	q.Push(model.NewTask("t1", "x", "{}", 1))
	q.Push(model.NewTask("t2", "x", "{}", 1))

	err := q.Push(model.NewTask("t3", "x", "{}", 1))
	if err != ErrQueueFull {
		t.Errorf("expected ErrQueueFull, got %v", err)
	}
}
