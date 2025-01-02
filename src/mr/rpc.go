package mr

type TaskArgs struct{
	WokerId int
}

type TaskReply struct{
	TaskType string		// "map", "reduce", "wait", "exit"
	FileName string 
	TaskNum int
	NReduce int
	NMap int
}

type ReportArgs struct{
	TaskType string
	TaskNum int
}

type ReportReply struct{

}

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
