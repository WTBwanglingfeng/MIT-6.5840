package mr

import (
	"sync"
	"time"
)

type ByKey []KeyValue

type Job struct {
	ID                string
	MapTaskIDs        []string
	ReduceTaskIDs     []string
	CompletedMapCount uint32
	mu                sync.Mutex
}

type Task struct {
	id            string
	jobID         string
	attemptID     uint64
	taskType      TaskType
	state         TaskState
	partitionID   uint32
	files         map[string]struct{}
	mu            sync.RWMutex
	assignedAt    time.Time
	startedAt     time.Time
	retryCount    uint8
	maxRetryCount uint8
	timeout       time.Duration
}

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
