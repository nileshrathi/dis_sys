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

type task_staus struct {
	Type  int //map =1 reduce =2
	state int //not started =0 running=1 completed=2
}

type Master struct {
	// Your definitions here.
	file_names           []string
	nReduce              int
	nMap                 int
	map_task_assigned    []int //0 not assigned 1 assigned 2 completed
	reduce_task_assigned []int
	mapComplete          bool
	reduceComplete       bool
	mapTaskTime          []time.Time
	reduceTaskTime       []time.Time
	mutex                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//yellow line hatao
func (m *Master) GetArgs(args *ReqTaskArgs, reply *ReplyTaskArgs) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if !m.mapComplete {
		//get the map task
		for i := 0; i < m.nMap; i++ {
			t1 := time.Now()
			if m.map_task_assigned[i] == 0 || (m.map_task_assigned[i] == 1 && t1.Sub(m.mapTaskTime[i]).Seconds() > 10) {
				// if m.map_task_assigned[i] == 1 && t1.Sub(m.mapTaskTime[i]).Seconds() > 10 {
				// 	fmt.Printf("worker dead for long period of time\n")
				// }
				m.map_task_assigned[i] = 1
				m.mapTaskTime[i] = time.Now()
				reply.Type = 1
				reply.Arg = m.file_names[i]
				reply.Index = i
				reply.NReduce = m.nReduce
				return nil
			}

		}
	}

	if m.mapComplete && !m.reduceComplete {
		for i := 0; i < m.nReduce; i++ {
			t1 := time.Now()
			if m.reduce_task_assigned[i] == 0 || (m.reduce_task_assigned[i] == 1 && t1.Sub(m.reduceTaskTime[i]).Seconds() > 10) {
				m.reduce_task_assigned[i] = 1
				m.reduceTaskTime[i] = time.Now()
				reply.Type = 2
				reply.Index = i
				reply.NReduce = m.nReduce
				reply.NMap = m.nMap
				return nil
			}
		}

	}

	if m.mapComplete && m.reduceComplete {
		reply.Type = 3
		return nil
	}
	return nil

	// for reduceCount !=m. {
	// 	if m.reduce_task_assigned[i] == false {
	// 		reply.Type = 2
	// 		reply.NReduce = m.nReduce
	// 		reply.NMap = m.nMap
	// 		reply.Index = i
	// 		break
	// 	}
	// }

}

//need to handle concurrent calls to TaskComplete ans get Args
func (m *Master) TaskComplete(args *TaskExecuted, reply *Status) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if args.Type == 1 {
		//Map task
		//check if task is completed in desired time
		//if  not then mark task unassigned
		mapTaskIndex := args.Index
		t1 := time.Now()
		t2 := m.mapTaskTime[mapTaskIndex]
		diff := t1.Sub(t2).Seconds()
		currStatus := m.map_task_assigned[mapTaskIndex]

		if currStatus != 2 {
			if diff <= 10 {
				m.map_task_assigned[mapTaskIndex] = 2
				reply.Status = 1
			} else {
				m.map_task_assigned[mapTaskIndex] = 0
			}
		} else {
			//Already done
			reply.Status = 1
		}
		//check if it was the last map task
		count := 0
		for i := 0; i < m.nMap; i++ {
			if m.map_task_assigned[i] == 2 {
				count++
			} else {
				break
			}
		}
		// if all map task completed then mark map as completed
		if count == m.nMap {
			m.mapComplete = true
		}

	} else if args.Type == 2 {
		//Reduce task
		//check if rask compltedted in desired time
		//if not set reduce task unassigned
		reduceTaskIndex := args.Index
		t1 := time.Now()
		t2 := m.reduceTaskTime[reduceTaskIndex]
		diff := t1.Sub(t2).Seconds()
		currStatus := m.reduce_task_assigned[reduceTaskIndex]

		if currStatus != 2 {
			if diff <= 10 {
				m.reduce_task_assigned[reduceTaskIndex] = 2
				reply.Status = 1
			} else {
				m.reduce_task_assigned[reduceTaskIndex] = 0
			}
		} else {
			//task alteady done
			reply.Status = 1
		}

		//check if it was the last reduce task
		count := 0
		for i := 0; i < m.nReduce; i++ {
			if m.reduce_task_assigned[i] == 2 {
				count++
			} else {
				break
			}
		}

		if count == m.nReduce {
			m.reduceComplete = true
		}

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
	ret := m.mapComplete && m.reduceComplete

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
	m.map_task_assigned = make([]int, len(files))
	m.mapTaskTime = make([]time.Time, len(files))
	m.reduce_task_assigned = make([]int, nReduce)
	m.reduceTaskTime = make([]time.Time, nReduce)
	m.mapComplete = false
	m.reduceComplete = false

	m.server()
	return &m
}
