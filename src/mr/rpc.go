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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ReqTaskArgs struct {
	Type          int // //-1 not assigned 0-Map 1 Reduce
	Status        int //-1 not assigned
	Index_of_file int //-1 not assigned
}
type ReplyTaskArgs struct {
	Type    int
	Arg     string
	Index   int
	NReduce int
	NMap    int
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
