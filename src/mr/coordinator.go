package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files             []string       // files for processing
	nMap              int            // number of map tasks
	nReduce           int            // number of reduce tasks
	tasks             []Task         // tasks for monitoring
	intermediateFiles []string       // a slice of names of intermediate files
	phrase            OpPhrase       // current stage of the MapReduce task
	requestCh         chan TaskMsg   // channel of task requests from workers
	reportCh          chan ReportMsg // channel of reports about task from workers
	done              chan struct{}  // indicate the whole process ends
}

type ICoordinator interface {
	schedule()
	assignTask(task *Task)
	processReport(report *Report)
	requestHandler(req *interface{}, resp *Task) error
	reportHandler(req *Report, resp *interface{}) error
	setupWorkerRequest(task *Task)
	initMapPhrase()
}

const TimeOutVal = time.Second * 5 // 5 seconds
var NReduce int = -1

// Your code here -- RPC handlers for the worker to call.

// process and schedule request and report messages from `requestCh` and `reportCh`
func (c *Coordinator) schedule() {
	c.initMapPhrase()
	for {
		select {
		case msg := <-c.requestCh:
			c.assignTask(msg.task)
			msg.ok <- struct{}{}

		case msg := <-c.reportCh:
			c.processReport(msg.report)
			msg.ok <- struct{}{}
		}
	}
}

// handle request PRC call from workers
func (c *Coordinator) requestHandler(req *interface{}, resp *Task) error {
	msg := TaskMsg{
		task: resp,
		ok:   make(chan struct{}),
	} // setup message
	c.requestCh <- msg
	<-msg.ok
	return nil
}

// handle report PRC call from workers
func (c *Coordinator) reportHandler(req *Report, resp *interface{}) error {
	msg := ReportMsg{
		report: req,
		ok:     make(chan struct{}),
	} // setup message
	c.reportCh <- msg
	<-msg.ok
	return nil
}

// assign a task to the worker
//
// if `MapTask` remains -> assign `MapTask`
//
// if no `MapTask` remains, but there is still `MapTask` `InProgress` -> assign `WaitTask`
//
// if all tasks done -> send Done to workers
//
// if no pending tasks -> let workers Wait
func (c *Coordinator) assignTask(task *Task) {
	currentTaskType := c.selectCurrentTaskType()

	allTaskDone := true

	for _, taskItem := range c.tasks {
		// assign unassigned task to worker
		if taskItem.taskType == currentTaskType && taskItem.taskStatus == Unassigned {
			taskItem.taskStatus = InProgress // update `taskStatus`
			task = &taskItem
			break
		}
		if taskItem.taskType == currentTaskType && taskItem.taskStatus != Ack {
			allTaskDone = false
		}
	}

	c.updateCurrentPhrase(allTaskDone)

	// if all the current tasks are done or in progress
	waitTask := Task{taskType: WaitTask}
	task = &waitTask
}

// update phrase to:
//
// - `ReducePhrase` if all `Map` tasks are done
//
// - `DonePhrase` if all `Reduce` tasks are done
func (c *Coordinator) updateCurrentPhrase(allTaskDone bool) {
	if allTaskDone {
		if c.phrase == MapPhrase {
			c.phrase = ReducePhrase
		} else if c.phrase == ReducePhrase {
			c.phrase = DonePhrase
		}
	}
}

// select current task type
func (c *Coordinator) selectCurrentTaskType() TaskType {
	var currentTaskType TaskType
	if c.phrase == MapPhrase {
		currentTaskType = MapTask
	} else if c.phrase == ReducePhrase {
		currentTaskType = ReduceTask
	} else {
		currentTaskType = WaitTask
		log.Fatal(fmt.Sprintf("invalid coordinator phrase: %v", c.phrase))
	}
	return currentTaskType
}

// process worker's report:
//
// - if report is `Ack`, mark the task as `Ack`
//
// - otherwise, mark the task as `Unassigned`
func (c *Coordinator) processReport(report *Report) {
	if report.task.taskStatus == Ack {
		// update intermediateFiles of Coordinator
		c.updateInterFiles(report.interFilenames)
	} else {
		report.task.taskStatus = Unassigned
	}
}

func (c*Coordinator) updateInterFiles(interFiles []string) {
	c.intermediateFiles = append(c.intermediateFiles, interFiles...)
}

func (c *Coordinator) initMapPhrase() {
	c.phrase = MapPhrase
	go c.setupMapTasks()
}

func (c *Coordinator) setupMapTasks() {
	for i, file := range c.files {
		task := Task{
			id:         i,
			fileName:   file,
			taskType:   MapTask,
			taskStatus: Unassigned,
		}
		c.tasks = append(c.tasks, task)
	}
}

func (c *Coordinator) setupReduceTasks(interFilenames []string) {
	for 
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	NReduce = nReduce
	c := Coordinator{
		files:     files,
		nReduce:   nReduce,
		nMap:      len(files),
		tasks:     make([]Task, 0),
		phrase:    InitPhrase,
		requestCh: make(chan TaskMsg),
		reportCh:  make(chan ReportMsg),
		done:      make(chan struct{}),
	}

	// Your code here.

	c.server()
	return &c
}
