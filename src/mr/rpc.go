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
	Timeout
	Assigned
	Ack
)

// Struct of task assigned by coordinator to workers
type Task struct {
	Id         int        // id number of the task
	TaskType   TaskType   // operation request
	FileName   string     // filename of file to be processed by this task (map phrase only)
	FileNames  []string   // filenames of files to be processed by this task (reduce phrase only)
	TaskStatus TaskStatus // status of the task
	StartTime  time.Time  // start time of the task
	NReduce    int        // number of reduce tasks
}

type TaskMsg struct {
	Task *Task
	Ok   chan struct{}
}

// Struct of worker request
type TaskRequestReq struct {
}

type TaskReportResp struct {
}

// Struct of worker response
type Report struct {
	Task           *Task
	InterFilenames []string
}

type ReportMsg struct {
	Report *Report
	Ok     chan struct{}
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
