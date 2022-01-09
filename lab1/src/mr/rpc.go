package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type RequestWorkerIdArgs struct {
}

type RequestWorkerIdReply struct {
	WorkerId int
}

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Type     TaskType
	FilePath string
	TaskId   int
}

type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
	ReduceCount int
}

type ReportCompletedArgs struct {
	Type     TaskType
	TaskId   int
	WorkerId int
}

type ReportCompletedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
