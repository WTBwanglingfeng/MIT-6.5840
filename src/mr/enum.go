package mr

type TaskState uint32
type TaskType uint32
type OpResult uint32

const (
	Idle TaskState = iota
	Assigned
	InProgress
	Completed
	Failed
	Waiting
)

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	OpSucceeded OpResult = iota
	OpFailed
)

func (s TaskState) CanTransitionTo(next TaskState) bool {
	switch s {
	case Idle:
		return next == Assigned || next == Failed
	case Assigned:
		return next == InProgress || next == Idle || next == Failed
	case InProgress:
		return next == Completed || next == Idle || next == Failed
	case Completed:
		return false
	case Waiting:
		return next == Idle
	default:
		return false
	}
}

func (s TaskState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Assigned:
		return "Assigned"
	case InProgress:
		return "InProgress"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	case Waiting:
		return "Waiting"
	default:
		return "Unknown"
	}
}
