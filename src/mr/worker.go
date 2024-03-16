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
		response := requestTask()
		if !response.ok {
			return
		}
		task := response.task
		switch task.taskType {
		case MapTask:
			processMap(task, mapf)
		case ReduceTask:
			processReduce(task, reducef)
		case WaitTask:
			time.Sleep(1 * time.Second)
		case DoneTask:
			return
		default:
			panic(fmt.Sprintf("unexpected task type: %v", task.taskType))
		}
	}
}

func processMap(task *Task, mapf func(string, string) []KeyValue) {
	// intermediate := []KeyValue{}

	// open the file in the assigned task
	filename := task.fileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	// read content of the file
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// do map
	intermediate := mapf(filename, string(content))
	// intermediate = append(intermediate, kva...)

	// do sorting
	sort.Sort(ByKey(intermediate))

	// create temp file
	curIndex := -1

	// store intermediate into temp json file
	var enc *json.Encoder
	for _, kv := range intermediate {
		for ihash(kv.Key)%NReduce != curIndex {
			curIndex++
			file, _ := os.CreateTemp("", fmt.Sprintf("mr-%v-%v", task.id, curIndex))
			enc = json.NewEncoder(file)
		}
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}
	// return
}

func processReduce(task *Task, reducef func(string, []string) string) {
	// open the intermediate file in the assigned task
	file, err := os.Open(task.fileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.fileName)
	}

	// decode the intermediate file
	var intermediate []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}

	numReduceTask := task.fileName[len(task.fileName)-1]
	oname := fmt.Sprintf("mr-out-%v", numReduceTask)
	ofile, _ := os.Create(oname)
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

// allow workers to request tasks from coordinator through 	`requestCh`
func requestTask(requestCh chan Task) TaskMsg {
	args := Request{}
	reply := TaskMsg{}

	ok := call("Coordinator.RPCHandler", &args, &reply)
	reply.task = <-requestCh

	reply.ok = ok

	return reply
}

// allow workers to send task reports to coordinator through `reportCh`
func reportTask(ack int8, filenames []string, taskType TaskType, reportCh chan Report) {
	report := Report{
		ack:            ack,
		taskType:       taskType,
		interFilenames: filenames,
	}

	reportCh <- report
}

func checkTimeout(task *Task) bool {
	if time.Since(task.startTime) > TimeOutVal {
		return true
	}
	return false
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
