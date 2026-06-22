package api

type CreateTaskRequest struct{
	ID string `json:"id,omitempty"`
	Type string `json:"type"`
	Payload string `json:"payload"`
	Priority int `json:"priority"`
	DelaySecs int `json:"delay_seconds,omitempty"`
	MaxRetries int `json:"max_retries,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
}

type TaskResponse struct{
	ID string `json:"id"`
	Type string `json:"type"`
	Status string `json:"status"`
	Priority int `json:"priority"`
	Attempts int `json:"attempts"`
	MaxRetries int `json:"max_retries"`
	LastError string `json:"last_error,omitempty"`
	CreatedAt string `json:"created_at"`
}

type StatsResponse struct{
	ReadyCount int `json:"ready_count"`
	DelayedCount int `json:"delayed_count"`
	DeadCount int `json:"dead_count"`
}
