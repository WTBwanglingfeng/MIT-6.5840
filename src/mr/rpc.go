package mr

import (
	"fmt"
	"sort"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type IntermediateFilesList []IntermediateFiles

type IntermediateFiles struct {
	PartitionID uint32
	FilePaths   map[string]struct{}
}

type GetTaskArgs struct{}

type GetTaskReply struct {
	HasTask     bool
	TaskID      string
	AttemptID   uint64
	TaskType    TaskType
	TaskState   TaskState
	Files       []string
	NReduce     uint32
	PartitionID uint32
}

type TransitionTaskStateArgs struct {
	TaskID    string
	TaskState TaskState
	AttemptID uint64
}

type TransitionTaskStateReply struct {
	OpResult  OpResult
	Message   string
	AttemptID uint64
}

type ReportMapOutputArgs struct {
	TaskID                string
	AttemptID             uint64
	IntermediateFilesList IntermediateFilesList
}

type ReportMapOutputReply struct {
	OpResult  OpResult
	Message   string
	AttemptID uint64
}

type RenameOutputFileArgs struct {
	RfileName string
	OfileName string
	TaskID    string
	AttemptID uint64
}

type RenameOutputFileReply struct {
	OpResult  OpResult
	Message   string
	AttemptID uint64
}

func (f IntermediateFiles) String() string {
	keys := make([]string, 0, len(f.FilePaths))
	for k := range f.FilePaths {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprintf("Partition %d: %v", f.PartitionID, keys)
}
