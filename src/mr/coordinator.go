package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	jobMap       map[string]*Job
	taskMap      map[string]*Task
	idleTasks    chan *Task
	nReduce      uint32
	taskMapMutex sync.RWMutex
	jobMapMutex  sync.RWMutex
}

const (
	defaultMaxRetryCount uint8 = 20
	defaultTaskTimeout         = 10 * time.Second
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	tasks := c.snapshotTasks()

	// Monitor task state and reassign failed or timed-out tasks.
	for _, task := range tasks {
		task.mu.Lock()
		state := task.state
		if state == Failed {
			c.retryTask(task, false)
		}

		if state == Assigned || state == InProgress {
			c.retryTaskIfTimedOut(task)
		}

		ret = (state == Completed) && ret
		task.mu.Unlock()
	}

	return ret
}

func (c *Coordinator) retryTaskIfTimedOut(task *Task) {
	if task.state == Assigned && time.Since(task.assignedAt) > task.timeout {
		log.Printf("task %s assignment timeout. assignedAt: %v, retryCount=%d, timeout=%v",
			task.id, task.assignedAt, task.retryCount, task.timeout)
		c.retryTask(task, true)
	}
	if task.state == InProgress && time.Since(task.startedAt) > task.timeout {
		log.Printf("task %s execution timeout. startedAt: %v, retryCount=%d, timeout=%v",
			task.id, task.startedAt, task.retryCount, task.timeout)
		c.retryTask(task, true)
	}
}

func (c *Coordinator) retryTask(task *Task, incrementAttemptID bool) {
	if task.retryCount >= task.maxRetryCount {
		log.Fatalf("task %s exceeded max retries. startedAt: %v, retryCount=%d, maxRetryCount=%d",
			task.id, task.startedAt, task.retryCount, task.maxRetryCount)
	}

	// Reset task state to Idle and increment the attempt ID if requested.
	task.state = Idle
	if incrementAttemptID {
		task.attemptID++
	}
	c.idleTasks <- task
	task.retryCount++
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Pop a task from the idle task channel.
	for {
		select {
		case task := <-c.idleTasks:
			task.mu.Lock()
			if task.state == Idle {
				reply.HasTask = true
				reply.TaskID = task.id
				task.attemptID++
				reply.AttemptID = task.attemptID
				reply.TaskType = task.taskType
				task.state = Assigned
				task.assignedAt = time.Now()
				reply.TaskState = task.state
				reply.PartitionID = task.partitionID
				// Convert task file paths from set to slice.
				files := make([]string, 0, len(task.files))
				for file := range task.files {
					files = append(files, file)
				}
				reply.Files = files
				reply.NReduce = c.nReduce
				task.mu.Unlock()
				return nil
			}
			task.mu.Unlock()
		default:
			return nil
		}

	}
}

func (c *Coordinator) TransitionTaskState(args *TransitionTaskStateArgs, reply *TransitionTaskStateReply) error {
	taskID := args.TaskID
	attemptID := args.AttemptID
	taskState := args.TaskState
	if taskState == Completed {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("completion requires output submission. taskID: %s, attemptID: %d", taskID, attemptID)
		return nil
	}

	task, ok := c.findTask(args.TaskID)
	if !ok {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("task not found or nil. taskID: %s, attemptID: %d", taskID, attemptID)
		return nil
	}

	task.mu.Lock()
	defer task.mu.Unlock()

	if !task.state.CanTransitionTo(taskState) {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("invalid transition: %v -> %v. taskID: %s, attemptID: %d", task.state, taskState, taskID, attemptID)
		return nil
	}

	if attemptID != task.attemptID {
		reply.OpResult = OpFailed
		taskAttemptID := task.attemptID
		reply.AttemptID = taskAttemptID
		reply.Message = fmt.Sprintf("stale attempt id. taskID: %s, attemptID: %d, current attemptID: %d", taskID, attemptID, taskAttemptID)
		return nil
	}

	task.state = taskState
	if task.state == InProgress {
		task.startedAt = time.Now()
	}

	reply.OpResult = OpSucceeded
	reply.AttemptID = task.attemptID
	return nil
}

func (c *Coordinator) recordMapCompletion(job *Job) error {
	job.mu.Lock()
	defer job.mu.Unlock()

	job.CompletedMapCount++
	return c.activateReduceTasks(job)
}

func (c *Coordinator) activateReduceTasks(job *Job) error {
	if job.CompletedMapCount != uint32(len(job.MapTaskIDs)) {
		return nil
	}

	// Activate Reduce tasks referenced by the job.
	for _, reduceTaskID := range job.ReduceTaskIDs {
		reduceTask, ok := c.findTask(reduceTaskID)
		if !ok {
			return fmt.Errorf("reduce task %q not found", reduceTaskID)
		}

		reduceTask.mu.Lock()
		curState := reduceTask.state
		if !curState.CanTransitionTo(Idle) {
			reduceTask.mu.Unlock()
			return fmt.Errorf("invalid transition: %v -> %v. reduceTaskID: %s", curState, Idle, reduceTaskID)
		}
		reduceTask.state = Idle
		// Enqueue Reduce task on the idle task channel.
		c.idleTasks <- reduceTask
		reduceTask.mu.Unlock()
	}
	return nil
}

func (c *Coordinator) ReportMapOutput(args *ReportMapOutputArgs, reply *ReportMapOutputReply) error {
	taskID := args.TaskID
	attemptID := args.AttemptID

	task, ok := c.findTask(taskID)
	if !ok {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("task not found or nil. taskID: %s, attemptID: %d", taskID, attemptID)
		return nil
	}

	task.mu.Lock()
	defer task.mu.Unlock()

	// Reject stale attempt IDs.
	if attemptID != task.attemptID {
		reply.OpResult = OpFailed
		taskAttemptID := task.attemptID
		reply.AttemptID = taskAttemptID
		reply.Message = fmt.Sprintf("stale attempt id. taskID: %s, attemptID: %d, current attemptID: %d", taskID, attemptID, taskAttemptID)
		return nil
	}

	if task.taskType != MapTask {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("map output requires a map task. taskID: %s, attemptID: %d", taskID, attemptID)
		return nil
	}
	// Ensure idempotent handling of duplicate invocations.
	if task.state == Completed {
		reply.OpResult = OpSucceeded
		reply.AttemptID = task.attemptID
		return nil
	}

	job, ok := c.findJob(task.jobID)
	if !ok {
		if task.state == InProgress {
			task.state = Failed
		}
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("job %s not found or nil. taskID: %s, attemptID: %d", task.jobID, taskID, attemptID)
		return nil
	}

	if !task.state.CanTransitionTo(Completed) {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("invalid transition: %v -> %v. taskID: %s, attemptID: %d", task.state, Completed, taskID, attemptID)
		return nil
	}

	// Validate partition IDs and find Reduce task pointers before updating Reduce task file paths.
	iFilesList := args.IntermediateFilesList
	reduceTasks := make([]*Task, len(iFilesList))
	for i, intermediateFile := range iFilesList {
		partitionID := intermediateFile.PartitionID
		if uint64(partitionID) >= uint64(len(job.ReduceTaskIDs)) {
			if task.state == InProgress {
				task.state = Failed
			}
			reply.OpResult = OpFailed
			reply.Message = fmt.Sprintf("invalid partition: %d", partitionID)
			return nil
		}

		reduceTask, ok := c.findTask(job.ReduceTaskIDs[partitionID])
		if !ok {
			reply.OpResult = OpFailed
			reply.Message = fmt.Sprintf("task not found or nil. taskID: %s, attemptID: %d, reduceTaskID: %s", taskID, attemptID, job.ReduceTaskIDs[partitionID])
			return nil
		}
		reduceTasks[i] = reduceTask
	}

	for i, intermediateFile := range iFilesList {
		reduceTask := reduceTasks[i]
		reduceTask.mu.Lock()
		for filePath := range intermediateFile.FilePaths {
			reduceTask.files[filePath] = struct{}{}
		}
		reduceTask.mu.Unlock()

	}

	task.state = Completed
	if err := c.recordMapCompletion(job); err != nil {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("%v. taskID: %s, attemptID: %d", err, taskID, attemptID)
		return nil
	}
	reply.OpResult = OpSucceeded
	reply.AttemptID = task.attemptID
	return nil
}

func (c *Coordinator) RenameOutputFile(args *RenameOutputFileArgs, reply *RenameOutputFileReply) error {
	taskID := args.TaskID
	attemptID := args.AttemptID

	task, ok := c.findTask(taskID)
	if !ok {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("task not found or nil. taskID: %s, attemptID: %d", taskID, attemptID)
		return nil
	}

	task.mu.Lock()
	defer task.mu.Unlock()

	// Reject stale attempt IDs.
	if attemptID != task.attemptID {
		reply.OpResult = OpFailed
		taskAttemptID := task.attemptID
		reply.AttemptID = taskAttemptID
		reply.Message = fmt.Sprintf("stale attempt id. taskID: %s, attemptID: %d, current attemptID: %d", taskID, attemptID, taskAttemptID)
		return nil
	}

	if task.taskType != ReduceTask {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("output rename requires a reduce task. taskID: %s, attemptID: %d", taskID, attemptID)
		return nil
	}
	// Ensure idempotent handling of duplicate invocations.
	if task.state == Completed {
		reply.OpResult = OpSucceeded
		reply.AttemptID = task.attemptID
		return nil
	}

	if !task.state.CanTransitionTo(Completed) {
		reply.OpResult = OpFailed
		reply.Message = fmt.Sprintf("invalid transition: %v -> %v. taskID: %s, attemptID: %d", task.state, Completed, taskID, attemptID)
		return nil
	}

	// Rename the output file.
	ofileName := args.OfileName
	rfileName := args.RfileName
	err := os.Rename(rfileName, ofileName)
	if err != nil {
		if task.state == InProgress {
			task.state = Failed
		}
		reply.OpResult = OpFailed
		taskAttemptID := task.attemptID
		reply.AttemptID = taskAttemptID
		reply.Message = fmt.Sprintf("rename file %s to %s. taskID: %s, attemptID: %d, current attemptID: %d",
			rfileName, ofileName, taskID, attemptID, taskAttemptID)
		return nil
	}

	task.state = Completed
	reply.OpResult = OpSucceeded
	reply.AttemptID = task.attemptID

	return nil
}

func newTask(id, jobID string, taskType TaskType, state TaskState, partitionID uint32, files map[string]struct{}) *Task {
	return &Task{
		id:            id,
		jobID:         jobID,
		taskType:      taskType,
		state:         state,
		partitionID:   partitionID,
		files:         files,
		maxRetryCount: defaultMaxRetryCount,
		timeout:       defaultTaskTimeout,
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:   uint32(nReduce),
		taskMap:   make(map[string]*Task),
		jobMap:    make(map[string]*Job),
		idleTasks: make(chan *Task, len(files)+nReduce),
	}

	jobID, err := newUUID()
	if err != nil {
		log.Fatalf("generate jobID failed. %v", err)
	}
	c.jobMap[jobID] = &Job{
		ID:                jobID,
		MapTaskIDs:        []string{},
		ReduceTaskIDs:     []string{},
		CompletedMapCount: 0,
	}

	// Initialize Map tasks and enqueue them on the idle task channel.
	for i := 0; i < len(files); i++ {
		taskID, err := newUUID()
		if err != nil {
			log.Fatalf("generate taskID failed. %v", err)
		}

		task := newTask(taskID, jobID, MapTask, Idle, 0, map[string]struct{}{files[i]: {}})
		c.taskMap[taskID] = task
		c.idleTasks <- task
		c.jobMap[jobID].MapTaskIDs = append(c.jobMap[jobID].MapTaskIDs, taskID)
	}

	// Initialize Reduce tasks in the Waiting state.
	for i := 0; i < nReduce; i++ {
		taskID, err := newUUID()
		if err != nil {
			log.Fatalf("generate taskID failed. %v", err)
		}

		task := newTask(taskID, jobID, ReduceTask, Waiting, uint32(i), make(map[string]struct{}))
		c.taskMap[taskID] = task
		c.jobMap[jobID].ReduceTaskIDs = append(c.jobMap[jobID].ReduceTaskIDs, taskID)
	}

	c.server(sockname)
	return &c
}

func (c *Coordinator) findTask(taskID string) (*Task, bool) {
	c.taskMapMutex.RLock()
	defer c.taskMapMutex.RUnlock()

	task, ok := c.taskMap[taskID]
	return task, ok && task != nil
}

func (c *Coordinator) findJob(jobID string) (*Job, bool) {
	c.jobMapMutex.RLock()
	defer c.jobMapMutex.RUnlock()

	job, ok := c.jobMap[jobID]
	return job, ok && job != nil
}

func (c *Coordinator) snapshotTasks() []*Task {
	c.taskMapMutex.RLock()
	defer c.taskMapMutex.RUnlock()

	// Snapshot task pointers under the map lock.
	tasks := make([]*Task, 0, len(c.taskMap))
	for _, task := range c.taskMap {
		tasks = append(tasks, task)
	}
	return tasks
}
