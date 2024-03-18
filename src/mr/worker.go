package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// declare an argument structure.
	for {
		task := requestTask()

		switch task.TaskType {
		case MapTask:
			processMap(task, mapf)
		case ReduceTask:
			processReduce(task, reducef)
		case WaitTask:
			time.Sleep(1 * time.Second)
		case DoneTask:
			return
		default:
			panic(fmt.Sprintf("unexpected task type: %v", task.TaskType))
		}
	}
}

func processMap(task *Task, mapf func(string, string) []KeyValue) {
	// open the file in the assigned task
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("processMap: cannot open %v", filename)
	}

	// read content of the file
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// do map
	kva := mapf(filename, string(content))

	intermediate := make(map[int][]KeyValue, task.NReduce)

	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	var filenames []string

	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", task.Id, r)
		ofile, _ := os.CreateTemp("", fmt.Sprintf("%v*", oname))
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
		filenames = append(filenames, oname)
	}

	task.TaskStatus = Ack
	reportTask(task, filenames)
}

func processReduce(task *Task, reducef func(string, []string) string) {
	// open the intermediate file in the assigned task
	intermediate := []KeyValue{}
	for _, filename := range task.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// decode the intermediate file
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// create output file
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := os.CreateTemp("", fmt.Sprintf("%v*", oname))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// close(done)
	os.Rename(ofile.Name(), oname)

	task.TaskStatus = Ack

	reportTask(task, make([]string, 0))
}

// allow workers to request tasks from coordinator through 	`requestCh`
func requestTask() *Task {
	args := &Task{}
	reply := &Task{}

	ok := call("Coordinator.RequestHandler", args, reply)

	if !ok {
		log.Fatal("cannot call Coordinator.RequestHandler")
	}
	return reply
}

// allow workers to send task reports to coordinator through `reportCh`
func reportTask(task *Task, filenames []string) {
	args := &Report{
		Task:           task,
		InterFilenames: filenames,
	}
	reply := &Report{}

	call("Coordinator.ReportHandler", args, reply)
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
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
