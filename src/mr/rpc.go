package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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
// var ACK int8 = 1
// var NACK int8 = 0

// Enum of task types
type TaskType int8

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	DoneTask
)

// Enum of Phrases of MapReduce process
type OpPhrase int8

const (
	InitPhrase OpPhrase = iota
	MapPhrase
	ReducePhrase
	DonePhrase
)

// Enum of task status
type TaskStatus int8

const (
	Unassigned TaskStatus = iota
	InProgress
	Timeout
	Nack
	Ack
)

// Struct of task assigned by coordinator to workers
type Task struct {
	id         int        // id number of the task
	taskType   TaskType   // operation request
	fileName   string     // filename of file to be processed by this task
	taskStatus TaskStatus // status of the task
	startTime  time.Time  // start time of the task
	nReduce    int        // number of reduce tasks
}

type TaskMsg struct {
	task *Task
	ok   chan struct{}
}

// Struct of worker request
// type TaskRequestReq struct {
// }

// Struct of worker request
type TaskRequestResp struct {
	task *Task
	ok   chan struct{}
}

// Struct of worker response
type Report struct {
	task           *Task
	interFilenames []string
}

type ReportMsg struct {
	report *Report
	ok     chan struct{}
}

// Struct of coordinator response to workers
type Response struct {
	TaskType  TaskType
	Filenames []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
