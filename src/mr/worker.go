package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	defer func() {
		if r := recover(); r != nil {
			//log.Printf("Worker crashed: %v", r)
			return
		}
	}()

	workerId := os.Getpid()
	//log.Printf("Worker %v starting", workerId)

	for {
		task := requestTask(workerId)

		if task == nil {
			//log.Printf("Worker %v got nil task, exiting", workerId)
			return
		}

		//4log.Printf("Worker %v got task: type=%v, num=%v", workerId, task.TaskType, task.TaskNum)

		switch task.TaskType {
		case "map":
			//log.Printf("Worker %v starting map task %v on file %v", workerId, task.TaskNum, task.FileName)
			doMap(task, mapf)
			//log.Printf("Worker %v completed map task %v", workerId, task.TaskNum)
		case "reduce":
			//log.Printf("Worker %v starting reduce task %v", workerId, task.TaskNum)
			doReduce(task, reducef)
			//log.Printf("Worker %v completed reduce task %v", workerId, task.TaskNum)
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			//log.Printf("Worker %v exiting", workerId)
			return
		}
	}
}

func requestTask(workerId int) *TaskReply {

	args := TaskArgs{WorkerId: workerId}
	reply := TaskReply{}

	if ok := call("Coordinator.AssignTask", &args, &reply); !ok {
		return nil
	}
	return &reply
}

func doMap(task *TaskReply, mapf func(string, string) []KeyValue) {
	defer func() {
		if r := recover(); r != nil {
			reportTask("map", task.TaskNum, false)
			return
		}
	}()

	content, err := os.ReadFile(task.FileName)
	if err != nil {
		reportTask("map", task.TaskNum, false)
		return
	}

	kva := mapf(task.FileName, string(content))

	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		tempFile := fmt.Sprintf("mr-%d-%d-%d.tmp", task.TaskNum, i, os.Getpid())
		file, err := os.Create(tempFile)
		if err != nil {
			reportTask("map", task.TaskNum, false)
			return
			//log.Fatalf("Cannot create temp file: %v", err)
		}

		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			if err := enc.Encode(&kv); err != nil {
				reportTask("map", task.TaskNum, false)
				return
				//log.Fatalf("Failed to encode kv pair: %v", err)
			}
		}
		file.Close()

		finalName := fmt.Sprintf("mr-%d-%d", task.TaskNum, i)
		if err := os.Rename(tempFile, finalName); err != nil {
			reportTask("map", task.TaskNum, false)
			return
			//log.Fatalf("Failed to rename map output file: %v", err)
		}
	}

	reportTask("map", task.TaskNum, true)
}

func doReduce(task *TaskReply, reducef func(string, []string) string) {
	defer func() {
		if r := recover(); r != nil {
			reportTask("reduce", task.TaskNum, false)
			return
		}
	}()

	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskNum)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}
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

	tempFile := fmt.Sprintf("mr-out-%d-%d.tmp", task.TaskNum, os.Getpid())
	ofile, err := os.Create(tempFile)
	if err != nil {
		reportTask("reduce", task.TaskNum, false)
		return
	}

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
	ofile.Close()

	finalName := fmt.Sprintf("mr-out-%d", task.TaskNum)
	if err := os.Rename(tempFile, finalName); err != nil {
		reportTask("reduce", task.TaskNum, false)
		return
	}

	reportTask("reduce", task.TaskNum, true)
}

func reportTask(taskType string, taskNum int, success bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Report task crashed: %v", r)
		}
	}()

	args := ReportArgs{
		TaskType: taskType,
		TaskNum:  taskNum,
		Success:  success,
	}
	reply := ReportReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Printf("dialing error: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
