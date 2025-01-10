package mr

import (
	"os"
	"strconv"
)

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	TaskType string // "map", "reduce", "wait", "exit"
	FileName string
	TaskNum  int
	NReduce  int
	NMap     int
	Error    string
}

type ReportArgs struct {
	TaskType string
	TaskNum  int
	WorkerId int
	Success  bool
}

type ReportReply struct {
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
