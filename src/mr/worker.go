package mr

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
const ofilePrefix = "mr-out-"
const ifilePrefix = "mr-"
const reduceFilePrefix = "mr-reduce-"
const channelBuffer = 10
const intervalTime = 100 * time.Millisecond
const reduceTaskMaxConcurrency = 16

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// Your worker implementation here.
	for {
		// Request a task
		getTaskReply := GetTaskReply{}
		if err := call("Coordinator.GetTask", &GetTaskArgs{}, &getTaskReply); err != nil {
			log.Printf("get task failed: %v", err)
			return
		}

		if !getTaskReply.HasTask {
			time.Sleep(intervalTime)
			continue
		}

		if nReduce := getTaskReply.NReduce; nReduce == 0 {
			log.Printf("nReduce can't be 0.")
			return
		}
		taskType := getTaskReply.TaskType

		// Confirm assignment and transition to InProgress.
		if err := transitionTaskState(&TransitionTaskStateArgs{getTaskReply.TaskID, InProgress, getTaskReply.AttemptID}); err != nil {
			log.Printf("transition task state failed. %v", err)
			continue
		}

		switch taskType {
		case MapTask:
			if err := runMapTask(getTaskReply, mapf); err != nil {
				log.Printf("run map task failed: %v", err)
			}
		case ReduceTask:
			if err := runReduceTask(getTaskReply, reducef); err != nil {
				log.Printf("run reduce task failed: %v", err)
			}
		default:
			log.Printf("wrong task type: %v.", taskType)
			return
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func runMapTask(task GetTaskReply, mapf func(string, string) []KeyValue) error {
	files, err := doMapTask(task.Files, mapf, task.TaskID, task.AttemptID, task.NReduce)
	if err != nil {
		return markTaskFailed(task, fmt.Errorf("execute map: %w", err))
	}

	if err := reportMapOutput(&ReportMapOutputArgs{task.TaskID, task.AttemptID, files}); err != nil {
		return fmt.Errorf("submit map output: %w", err)
	}
	return nil
}

func runReduceTask(task GetTaskReply, reducef func(string, []string) string) error {
	rfileName, err := doReduceTask(task.Files, reducef, task.TaskID, task.AttemptID, task.PartitionID)
	if err != nil {
		return markTaskFailed(task, fmt.Errorf("execute reduce: %w", err))
	}

	rfileName, err = filepath.Abs(rfileName)
	if err != nil {
		return markTaskFailed(task, fmt.Errorf("resolve temporary output path: %w", err))
	}
	ofileName, err := filepath.Abs(ofilePrefix + strconv.Itoa(int(task.PartitionID)))
	if err != nil {
		return markTaskFailed(task, fmt.Errorf("resolve final output path: %w", err))
	}

	if err := renameOutputFile(&RenameOutputFileArgs{rfileName, ofileName, task.TaskID, task.AttemptID}); err != nil {
		return fmt.Errorf("submit reduce output: %w", err)
	}
	return nil
}

func markTaskFailed(task GetTaskReply, cause error) error {
	if err := transitionTaskState(&TransitionTaskStateArgs{task.TaskID, Failed, task.AttemptID}); err != nil {
		return errors.Join(cause, fmt.Errorf("mark task failed: %w", err))
	}
	return cause
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Example", &args, &reply)
	if err == nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed: %v\n", err)
	}
}

// send an RPC request to the coordinator and return any transport error.
func call(rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return fmt.Errorf("dial coordinator for %s: %w", rpcname, err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		return fmt.Errorf("RPC %s failed: %w", rpcname, err)
	}
	return nil
}

func doMapTask(files []string, mapf func(string, string) []KeyValue, taskID string, attemptID uint64, nReduce uint32) ([]IntermediateFiles, error) {
	// Read file content and apply mapf.
	intermediate, err := mapInputFiles(files, mapf)
	if err != nil {
		return nil, err
	}

	// Local sorting.
	sort.Sort(ByKey(intermediate))

	// Write intermediate files.
	fileResults := make([]IntermediateFiles, nReduce)
	for i := range fileResults {
		fileResults[i].PartitionID = uint32(i)
		fileResults[i].FilePaths = make(map[string]struct{})
	}
	// Initialize channels for each partition, send KVs to their corresponding channels whilst consuming them and writing to intermediate files.
	chs := make([]chan []KeyValue, nReduce)
	g, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < len(chs); i++ {
		chs[i] = make(chan []KeyValue, channelBuffer)
		i := i
		g.Go(func() error {
			// Intermediate file name pattern: mr-TaskID-AttemptID-PartitionID.
			fileName := ifilePrefix + taskID + "-" + strconv.Itoa(int(attemptID)) + "-" + strconv.Itoa(i)
			if err := os.Remove(fileName); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove intermediate file: %q failed: %w", fileName, err)
			}

			if err := writeMapPartition(ctx, fileName, chs[i]); err != nil {
				return err
			}
			fileResults[i].FilePaths[fileName] = struct{}{}
			return nil
		})
	}

	dispatchMapBatches(ctx, intermediate, chs)
	for _, ch := range chs {
		close(ch)
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("map task failed: %w", err)
	}
	return fileResults, nil
}

func mapInputFiles(files []string, mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	// This function needn't read files concurrently due to input files having only one file.
	intermediate := []KeyValue{}
	for _, file := range files {
		content, err := readFile(file)
		if err != nil {
			return nil, fmt.Errorf("read file failed: %w", err)
		}
		kva := mapf(file, string(content))
		intermediate = append(intermediate, kva...)
	}
	return intermediate, nil
}

func dispatchMapBatches(ctx context.Context, intermediate []KeyValue, chs []chan []KeyValue) {
	// Group KVs by key and send KVs to corresponding channels.
	for i := 0; i < len(intermediate); {
		kv := intermediate[i]
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == kv.Key {
			j++
		}

		bucket := ihash(kv.Key) % len(chs)
		select {
		case <-ctx.Done():
			return
		case chs[bucket] <- intermediate[i:j]:
			i = j
		}

	}
}

func writeMapPartition(ctx context.Context, fileName string, batches <-chan []KeyValue) (retErr error) {
	ofile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open intermediate file: %q failed: %w", fileName, err)
	}
	defer func() {
		if err := ofile.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("close intermediate file %q: %w", fileName, err)
		}
	}()

	// Buffer writes to reduce I/O calls.
	writer := bufio.NewWriter(ofile)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case intermediate, ok := <-batches:
			if !ok {
				return writer.Flush()
			}
			if err := writeKV(writer, intermediate); err != nil {
				return fmt.Errorf("write intermediate file failed: %w", err)
			}

		}
	}
}

func doReduceTask(files []string, reducef func(string, []string) string, taskID string, attemptID uint64, partitionID uint32) (string, error) {
	reduceFileName := reduceFilePrefix + taskID + "-" + strconv.Itoa(int(attemptID)) + "-" + strconv.Itoa(int(partitionID))
	if err := os.Remove(reduceFileName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("remove output file: %q failed: %w", reduceFileName, err)
	}

	// Read intermediate files, apply reducef, and write outputs to a temporary output file.
	kvas := make([][]KeyValue, len(files))
	if err := readIntermediateFiles(files, kvas); err != nil {
		return "", err
	}

	if err := reduceAndWrite(reduceFileName, reducef, kvas); err != nil {
		return "", err
	}

	return reduceFileName, nil
}

func readIntermediateFiles(files []string, kvas [][]KeyValue) error {
	var g errgroup.Group
	// Limit concurrent file reads.
	g.SetLimit(reduceTaskMaxConcurrency)

	// Read KVs from intermediate files.
	for i, file := range files {
		g.Go(func() error {
			ifile, err := os.OpenFile(file, os.O_RDONLY, 0644)
			if err != nil {
				return fmt.Errorf("open intermediate file: %q failed: %w", file, err)
			}
			defer ifile.Close()

			if err = readKV(ifile, &kvas[i]); err != nil {
				return fmt.Errorf("read file %q failed: %w", file, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("reduce task failed: %w", err)
	}
	return nil
}

func reduceAndWrite(reduceFileName string, reducef func(string, []string) string, kvas [][]KeyValue) (retErr error) {
	// Use a min-heap to speed up merging KVs from presorted files.
	candidates := newMergeHeap(kvas)

	rFile, err := os.OpenFile(reduceFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open output file: %q failed: %w", reduceFileName, err)
	}
	defer func() {
		if err := rFile.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("close reduce file %q: %w", reduceFileName, err)
		}
	}()

	// Buffer writes to reduce I/O calls.
	writer := bufio.NewWriter(rFile)
	for candidates.Len() > 0 {
		key := candidates[0].kv.Key
		values := collectKeyValues(&candidates, kvas, key)

		content := reducef(key, values)
		if _, err := fmt.Fprintf(writer, "%v %v\n", key, content); err != nil {
			return fmt.Errorf("write file %q failed: %w", reduceFileName, err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush reduce output: %w", err)
	}

	return nil
}

func transitionTaskState(args *TransitionTaskStateArgs) error {
	reply := TransitionTaskStateReply{}
	if err := call("Coordinator.TransitionTaskState", args, &reply); err != nil {
		return fmt.Errorf("transition task %s to %s (attempt %d): %w",
			args.TaskID, args.TaskState, args.AttemptID, err)
	}
	if reply.OpResult != OpSucceeded {
		return fmt.Errorf("coordinator rejected transition for task %s to %s (attempt %d): %s",
			args.TaskID, args.TaskState, args.AttemptID, reply.Message)
	}
	return nil
}

func reportMapOutput(args *ReportMapOutputArgs) error {
	reply := ReportMapOutputReply{}
	if err := call("Coordinator.ReportMapOutput", args, &reply); err != nil {
		return fmt.Errorf("report output for task %s (attempt %d): %w",
			args.TaskID, args.AttemptID, err)
	}
	if reply.OpResult != OpSucceeded {
		return fmt.Errorf("coordinator rejected output for task %s (attempt %d): %s",
			args.TaskID, args.AttemptID, reply.Message)
	}
	return nil
}

func renameOutputFile(args *RenameOutputFileArgs) error {
	reply := RenameOutputFileReply{}
	if err := call("Coordinator.RenameOutputFile", args, &reply); err != nil {
		return fmt.Errorf("rename output for task %s (attempt %d, %s -> %s): %w",
			args.TaskID, args.AttemptID, args.RfileName, args.OfileName, err)
	}
	if reply.OpResult != OpSucceeded {
		return fmt.Errorf("coordinator rejected output rename for task %s (attempt %d, %s -> %s): %s",
			args.TaskID, args.AttemptID, args.RfileName, args.OfileName, reply.Message)
	}
	return nil
}
