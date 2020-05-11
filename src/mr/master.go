package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type task_staus struct {
	Type  int //map =1 reduce =2
	state int //not started =0 running=1 completed=2
}

type Master struct {
	// Your definitions here.
	file_names           []string
	nReduce              int
	nMap                 int
	map_task_assigned    []bool
	reduce_task_assigned []bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

var count int

//yellow line hatao
func (m *Master) GetArgs(args *ReqTaskArgs, reply *ReplyTaskArgs) error {
	count++
	if count == 1 {
		for i := 0; i < len(m.map_task_assigned); i++ {
			if m.map_task_assigned[i] == false {
				reply.Type = 1
				reply.Arg = m.file_names[i]
				reply.Index = i
				reply.NReduce = m.nReduce
				m.map_task_assigned[i] = true
				break
			}

		}
	}

	if count == 2 {
		for i := 0; i < len(m.reduce_task_assigned); i++ {
			if m.reduce_task_assigned[i] == false {
				reply.Type = 2
				reply.NReduce = m.nReduce
				reply.NMap = m.nMap
				reply.Index = i
				break
			}
		}
	}

	if count == 3 {
		reply.Type = 3
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.

	m.file_names = files
	m.nReduce = nReduce
	m.nMap = len(files)
	m.map_task_assigned = make([]bool, len(files))
	m.reduce_task_assigned = make([]bool, nReduce)

	m.server()
	return &m
}
