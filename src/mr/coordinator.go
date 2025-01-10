package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int
type Phase int

const (
	TaskPending TaskStatus = iota
	TaskRunning
	TaskCompleted
)

const (
	TaskTimeout = 40 * time.Second
)

type TaskInfo struct {
	Status    TaskStatus
	StartTime time.Time
	FileName  string
	TaskNum   int
}

type Coordinator struct {
	files       []string
	nReduce     int
	mapTasks    []TaskInfo
	reduceTasks []TaskInfo
	mapMu       sync.RWMutex // 保护 mapTasks
	reduceMu    sync.RWMutex // 保护 reduceTasks
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]TaskInfo, len(files)),
		reduceTasks: make([]TaskInfo, nReduce),
	}

	for i := range c.mapTasks {
		c.mapTasks[i] = TaskInfo{
			Status:   TaskPending,
			FileName: files[i],
			TaskNum:  i,
		}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = TaskInfo{
			Status:  TaskPending,
			TaskNum: i,
		}
	}

	go c.checkTimeouts()

	c.server()
	return &c
}

func (c *Coordinator) checkTimeouts() {
	for !c.Done() {
		time.Sleep(1 * time.Second)

		// 检查 map 任务超时
		c.mapMu.Lock()
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == TaskRunning &&
				time.Since(c.mapTasks[i].StartTime) > TaskTimeout {
				c.mapTasks[i].Status = TaskPending
			}
		}
		c.mapMu.Unlock()

		// 检查 reduce 任务超时
		c.reduceMu.Lock()
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == TaskRunning &&
				time.Since(c.reduceTasks[i].StartTime) > TaskTimeout {
				c.reduceTasks[i].Status = TaskPending
			}
		}
		c.reduceMu.Unlock()
	}
}

func (c *Coordinator) Done() bool {

	for _, task := range c.mapTasks {
		if task.Status != TaskCompleted {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.Status != TaskCompleted {
			return false
		}
	}

	time.Sleep(5 * time.Second)
	return true

}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// 检查 map 任务
	c.mapMu.Lock()
	for i := range c.mapTasks {
		if c.mapTasks[i].Status == TaskPending {
			c.mapTasks[i].Status = TaskRunning
			c.mapTasks[i].StartTime = time.Now()
			reply.TaskType = "map"
			reply.TaskNum = c.mapTasks[i].TaskNum
			reply.FileName = c.mapTasks[i].FileName
			reply.NReduce = c.nReduce
			c.mapMu.Unlock()
			return nil
		}
	}

	// 检查是否有 map 任务在运行
	for _, task := range c.mapTasks {
		if task.Status != TaskCompleted {
			reply.TaskType = "wait"
			c.mapMu.Unlock()
			return nil
		}
	}
	c.mapMu.Unlock()
	//log.Println("map tasks all done")
	// 检查 reduce 任务
	c.reduceMu.Lock()
	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status == TaskPending {
			c.reduceTasks[i].Status = TaskRunning
			c.reduceTasks[i].StartTime = time.Now()
			reply.TaskType = "reduce"
			reply.TaskNum = c.reduceTasks[i].TaskNum
			reply.NMap = len(c.mapTasks)
			c.reduceMu.Unlock()
			return nil
		}
	}

	// 检查是否有 reduce 任务在运行
	for _, task := range c.reduceTasks {
		if task.Status != TaskCompleted {
			reply.TaskType = "wait"
			c.reduceMu.Unlock()
			return nil
		}
	}
	c.reduceMu.Unlock()

	reply.TaskType = "exit"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	if args.TaskType == "map" {
		c.mapMu.Lock()
		if args.Success && c.mapTasks[args.TaskNum].Status == TaskRunning {
			c.mapTasks[args.TaskNum].Status = TaskCompleted
		} else {
			c.mapTasks[args.TaskNum].Status = TaskPending
			c.mapTasks[args.TaskNum].StartTime = time.Time{}
		}
		c.mapMu.Unlock()

	} else if args.TaskType == "reduce" {
		c.reduceMu.Lock()
		if args.Success && c.reduceTasks[args.TaskNum].Status == TaskRunning {
			c.reduceTasks[args.TaskNum].Status = TaskCompleted
		} else {
			c.reduceTasks[args.TaskNum].Status = TaskPending
			c.reduceTasks[args.TaskNum].StartTime = time.Time{}
		}
		c.reduceMu.Unlock()

	}

	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}
