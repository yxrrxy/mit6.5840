package mr

import (
	"log"
	"sync"
	"time"
)

type TaskStatus int
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	CompletePhase
)

const (
	TaskPending TaskStatus = iota
	TaskRunning
	TaskCompleted
)

const (
	TaskTimeout = 10 * time.Second
)

type TaskInfo struct {
	Status    TaskStatus
	StartTime time.Time
	FileName  string
	TaskNum   int
	Attempts  int
}

type Coordinator struct {
	files       []string
	nReduce     int
	phase       Phase
	mapTasks    []TaskInfo
	reduceTasks []TaskInfo
	mu          sync.Mutex
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		phase:       MapPhase,
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
			Status:   TaskPending,
			FileName: files[i],
			TaskNum:  i,
		}
	}

	go c.checkTimeouts()

	c.server()
	return &c
}

func (c *Coordinator) checkTimeouts() {
	for !c.Done() {
		time.Sleep(time.Second)
		c.mu.Lock()

		if c.phase == MapPhase {
			for i := range c.mapTasks {
				if c.mapTasks[i].Status == TaskRunning &&
					time.Since(c.mapTasks[i].StartTime) > TaskTimeout {
					log.Printf("Map task %d timeout, resertting", i)
					c.mapTasks[i].Status = TaskPending
					c.mapTasks[i].Attempts++
				}
			}
		}

		c.mu.Unlock()
	}
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == CompletePhase
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updatePhase()

	switch c.phase {
	case MapPhase:
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == TaskPending {
				c.mapTasks[i].Status = TaskRunning
				c.mapTasks[i].StartTime = time.Now()

				reply.TaskType = "map"
				reply.FileName = c.mapTasks[i].FileName
				reply.TaskNum = i
				reply.NReduce = c.nReduce
				return nil
			}
		}
	case ReducePhase:
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == TaskPending {
				c.reduceTasks[i].Status = TaskRunning
				c.reduceTasks[i].StartTime = time.Now()

				reply.TaskType = "reduce"
				reply.TaskNum = i
				reply.NMap = len(c.mapTasks)
			}
		}

	case CompletePhase:
		reply.TaskType = "exit"
		return nil
	}

	reply.TaskType = "wait"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskArgs == "map" {
		if c.phase == MapPhase && c.mapTasks[args.TaskNum].Status == TaskRunning {
			c.mapTasks[args.TaskNum].Status = TaskCompleted
		}
	} else if args.TaskType == "reduce" {
		if c.phase == ReducePhase && c.reduceTasks[args.TaskNum].Status == TaskRunning {
			c.mapTasks[args.TaskNum].Status = TaskCompleted
		}
	}

	c.updatePhase()
	return nil
}

func (c *Coordinator) updatePhase() {
	if c.phase == MapPhase {
		allDone := true
		for _, task := range c.mapTasks {
			if task.Status != TaskCompleted {
				allDone = false
				break
			}
		}
		if allDone {
			c.phase = ReducePhase
		}
	} else if c.phase == ReducePhase {
		allDone := true
		for _, task := range c.reduceTasks {
			if task.Status != TaskCompleted {
				allDone = false
				break
			}
		}
		if allDone {
			c.phase = CompletePhase
		}
	}
}
