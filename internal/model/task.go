package model

import "time"

// id types status
// lifetime(status)
// scheduling info(priority, delay, when to run)
// retry info (max attempt, current attempt, backoffs)
// Dependency info(which job to finish first)
// Time stamp (for dashboard - in future)

// custom string(enum) -> preventing any random status string

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusReady     TaskStatus = "ready"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusDead      TaskStatus = "dead"
	StatusFailed    TaskStatus = "failed"

	StatusBlocked   TaskStatus = "blocked"
)

type Task struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`

	Status   TaskStatus `json:"status"`
	Priority int        `json:"priority"`

	RunAt time.Time `json:"run_at"`

	MaxRetries int `json:"max_retries"`
	Attempts   int `json:"attempt"`
	LastError  string `json:"last_error"`

	DependsOn []string `json:"depends_on"`

	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

// new Task - constructor
func NewTask(id, taskType, payload string, priority int) *Task {
	return &Task{
		ID:      id,
		Type:    taskType,
		Payload: payload,

		Status:   StatusPending,
		Priority: priority,

		RunAt: time.Now(),

		MaxRetries: 3,
		Attempts:   0,

		CreatedAt: time.Now(),
		DependsOn: []string{},
	}
}

// IsReady -> true if  passed its scheduled run time and has no unmet dependencies
func (t *Task) IsReady() bool {
	return time.Now().After(t.RunAt) || time.Now().Equal(t.RunAt)
}

// IsDelayed -> true if task should to wait before running
func (t *Task) IsDelayed() bool {
	return time.Now().Before(t.RunAt)
}

// CanRetry -> remaining Attempts or not
func (t *Task) CanRetry() bool {
	return t.Attempts < t.MaxRetries
}

// Lets keep exponential backoff 1-2-4-8-16-32-64-128-256-512-1024------
func (t *Task) NextBackoff() time.Duration {
	seconds := 1 << t.Attempts
	return time.Duration(seconds) * time.Second
}
