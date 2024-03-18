package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files   []string // files for processing
	nMap    int      // number of map tasks
	nReduce int      // number of reduce tasks
	tasks   []Task   // tasks for monitoring
	phrase    OpPhrase       // current stage of the MapReduce task
	requestCh chan TaskMsg   // channel of task requests from workers
	reportCh  chan ReportMsg // channel of reports about task from workers
	done      chan struct{}  // indicate the whole process ends
}

type ICoordinator interface {
	schedule()
	RequestHandler(req *interface{}, resp *Task) error
	ReportHandler(req *Report, resp *interface{}) error
	assignTask(task *Task)
	processReport(report *Report)
	updateCurrentPhrase(allTaskDone bool)
	selectCurrentTaskType() TaskType
	setupWorkerRequest(task *Task)
	initMapPhrase()
	setupMapTasks()
	setupReduceTasks()
	organizeInterFiles()
	initMapTask(i int, file string) Task
}

// Your code here -- RPC handlers for the worker to call.

// process and schedule request and report messages from `requestCh` and `reportCh`
func (c *Coordinator) schedule() {
	c.initMapPhrase()
	for {
		select {
		case msg := <-c.requestCh:
			c.assignTask(msg.Task)
			msg.Ok <- struct{}{}

		case msg := <-c.reportCh:
			c.processReport(msg.Report)
			msg.Ok <- struct{}{}
		}
	}
}

// handle request PRC call from workers
func (c *Coordinator) RequestHandler(req *Task, resp *Task) error {
	msg := TaskMsg{
		Task: resp,
		Ok:   make(chan struct{}),
	} // setup message
	c.requestCh <- msg
	<-msg.Ok
	return nil
}

// handle report PRC call from workers
func (c *Coordinator) ReportHandler(req *Report, resp *Report) error {
	msg := ReportMsg{
		Report: req,
		Ok:     make(chan struct{}),
	} // setup message
	c.reportCh <- msg
	<-msg.Ok
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
// func (c *Coordinator) assignTask() (*Task, bool) {
func (c *Coordinator) assignTask(task *Task) {
	currentTaskType := c.selectCurrentTaskType()

	if currentTaskType == DoneTask {
		*task = Task{TaskType: DoneTask}
		return
	}

	allTaskDone := true

	for i := range c.tasks {
		if c.tasks[i].TaskStatus == TaskStatus(Assigned) && time.Since(c.tasks[i].StartTime) > time.Second*10 {
			c.tasks[i].TaskStatus = TaskStatus(Unassigned)
		}

		// assign unassigned task to worker
		if c.tasks[i].TaskType == currentTaskType && c.tasks[i].TaskStatus == TaskStatus(Unassigned) {
			c.tasks[i].TaskStatus = TaskStatus(Assigned) // update `taskStatus`
			c.tasks[i].StartTime = time.Now()
			*task = c.tasks[i]
			return
		}

		if c.tasks[i].TaskType == currentTaskType && c.tasks[i].TaskStatus != TaskStatus(Ack) {
			allTaskDone = false
		}
	}

	c.updateCurrentPhrase(allTaskDone)

	// if all the current tasks are done or in progress
	*task = Task{TaskType: WaitTask}
}

// process worker's report:
//
// - if report is `Ack`, mark the task as `Ack`
//
// - otherwise, mark the task as `Unassigned`
func (c *Coordinator) processReport(report *Report) {
	if report.Task.TaskStatus == Ack {
		for i, taskItem := range c.tasks {
			if taskItem.Id == report.Task.Id {
				c.tasks[i].TaskStatus = Ack
			}
		}

		// update files of Coordinator
		c.files = append(c.files, report.InterFilenames...)
	} else {
		report.Task.TaskStatus = Unassigned
	}
}

// update phrase to:
//
// - `ReducePhrase` if all `Map` tasks are done
//
// - `DonePhrase` if all `Reduce` tasks are done
func (c *Coordinator) updateCurrentPhrase(allTaskDone bool) {
	if allTaskDone {
		if c.phrase == MapPhrase {
			// setup Reduce tasks
			c.setupReduceTasks()
			c.phrase = ReducePhrase
		} else if c.phrase == ReducePhrase {
			// c.removeTempFiles()
			c.phrase = DonePhrase
		}
	}
}

// func (c *Coordinator) removeTempFiles() {
// 	for _, file := range c.files {
// 		e := os.Remove(file)
// 		if e != nil {
// 			log.Fatal(e)
// 		}
// 	}
// }

// select current task type
func (c *Coordinator) selectCurrentTaskType() TaskType {
	var currentTaskType TaskType
	if c.phrase == MapPhrase {
		currentTaskType = MapTask
	} else if c.phrase == ReducePhrase {
		currentTaskType = ReduceTask
	} else if c.phrase == DonePhrase {
		currentTaskType = DoneTask
	} else {
		currentTaskType = WaitTask
		log.Fatalf(fmt.Sprintf("invalid coordinator phrase: %v", c.phrase))
	}
	return currentTaskType
}

func (c *Coordinator) initMapPhrase() {
	c.phrase = MapPhrase
	c.setupMapTasks()
}

func (c *Coordinator) setupMapTasks() {
	for i, file := range c.files {
		task := c.initMapTask(i, file)
		c.tasks = append(c.tasks, task)
	}
	c.files = nil //  clear c.files
}

func (c *Coordinator) setupReduceTasks() {
	reduceTaskFiles := c.organizeInterFiles()

	for reduceTaskNum, files := range reduceTaskFiles {
		task := initReduceTask(reduceTaskNum, files)
		c.tasks = append(c.tasks, task)
	}
}

// reorganize intermediate files by `reduceTaskNum`
//
// return a map(reduceTaskNum, files)
func (c *Coordinator) organizeInterFiles() map[int][]string {
	reduceTaskFiles := make(map[int][]string)
	for _, filename := range c.files {
		// Extract X and Y from filename (e.g., "mr-1-2")
		parts := strings.Split(filename, "-")
		reduceTaskNum, _ := strconv.Atoi(parts[2])

		// Assign filename to the correct reduce task list
		reduceTaskFiles[reduceTaskNum] = append(reduceTaskFiles[reduceTaskNum], filename)
	}

	return reduceTaskFiles
}

func (c *Coordinator) initMapTask(i int, file string) Task {
	task := Task{
		Id:         i,
		FileName:   file,
		TaskType:   MapTask,
		TaskStatus: Unassigned,
		StartTime:  time.Now(),
		NReduce:    c.nReduce,
	}
	return task
}

func initReduceTask(i int, files []string) Task {
	task := Task{
		Id:         i,
		FileNames:  files,
		TaskType:   ReduceTask,
		StartTime:  time.Now(),
		TaskStatus: Unassigned,
	}
	return task
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
	return c.phrase == OpPhrase(DonePhrase)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
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
	go c.schedule()

	return &c
}
