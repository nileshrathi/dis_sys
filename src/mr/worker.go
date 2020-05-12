package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) } //interface in golang
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	for {
		taskreply, error := get_args_from_master()
		if error != nil {
			log.Fatalf("could not get task %v\n", error)
			continue
		}
		if taskreply.Type == 1 {
			//map Task
			result := runMap(mapf, taskreply.Arg, taskreply.Index, taskreply.NReduce)
			if result == 0 {
				fmt.Printf("Failed to perform map\n")
			} else if result == 1 {
				//	fmt.Printf("Map pahase executed succesfully\n")
			}
			callTaskExecuted(1, taskreply.Index)
		} else if taskreply.Type == 2 {
			//Reduce Task
			result := runReduce(reducef, taskreply.Index, taskreply.NReduce, taskreply.NMap)
			if result == 0 {
				fmt.Printf("Failed to perform Reduce\n")
			} else if result == 1 {
				//	fmt.Printf("Reduce performed successfully\n")
			}
			callTaskExecuted(2, taskreply.Index)
		} else if taskreply.Type == 3 {
			//	fmt.Printf("MAP-REDUCE executed SUCCESSFULY")
			break
		}
		time.Sleep(2 * time.Second)

	}

}

func get_args_from_master() (ReplyTaskArgs, error) {
	args := ReqTaskArgs{}
	args.Type = -1
	args.Status = -1
	args.Index_of_file = -1
	reply := ReplyTaskArgs{}
	call("Master.GetArgs", &args, &reply)
	return reply, nil
}

func callTaskExecuted(Type int, Index int) (Status, error) {
	args := TaskExecuted{}
	args.Type = Type
	args.Index = Index
	reply := Status{}
	for i := 0; i < 10; i++ {
		call("Master.TaskComplete", &args, &reply)
		if reply.Status == 1 {
			break
		}
		time.Sleep(2 * time.Second)
	}

	return reply, nil
}

func runMap(mapf func(string, string) []KeyValue,
	fileName string, mapTaskIndex int, NReduce int) int {
	//open the file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("can not open file %v\n", err)
	}
	//read content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("can not read content %v\n", err)
	}

	tempkva := mapf(fileName, string(content))
	//write to files
	result := writeMapOutput(tempkva, mapTaskIndex, NReduce)
	return result
}

func writeMapOutput(kv []KeyValue, mapTaskIndex int, NReduce int) int {
	//create files
	fileArray := make([](*os.File), NReduce)
	for i := 0; i < NReduce; i++ {
		filename := "mr-" + strconv.Itoa(mapTaskIndex) + "-" + strconv.Itoa(i)
		f, err := os.Create(filename)
		fileArray[i] = f
		if err != nil {
			log.Fatalf("unable to create intermediate file %v filename %v\n", err, filename)
			return 0
		}
	}
	//writes to files
	for i := 0; i < len(kv); i++ {
		//which file to write to
		reduceTaskNo := ihash(kv[i].Key) % NReduce

		//encode the key value
		enc := json.NewEncoder(fileArray[reduceTaskNo])
		err := enc.Encode(&kv[i])
		if err != nil {
			log.Fatalf("error occoured while encoding %v\n", err)
			return 0
		}
	}
	//close all the files

	for i := 0; i < NReduce; i++ {
		fileArray[i].Close()
	}
	return 1

}

func runReduce(reducef func(string, []string) string, reduceIndex int, NReduce int, NMap int) int {

	intermediate := getIntermediateValues(reduceIndex, NReduce, NMap)
	oname := "mr-out-" + strconv.Itoa(reduceIndex)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("error while opening file %v\n", err)
		return 0
	}

	// collect values blongint to the same Key
	// and call reducef on key,[]values

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
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
	err = ofile.Close()
	if err != nil {
		log.Fatalf("error occoured while closig file %v\n", err)
		return 0
	}
	return 1

}
func getIntermediateValues(reduceIndex int, NReduce int, NMap int) []KeyValue {

	//get all file names
	kva := make([]KeyValue, 0)
	//fileNameArray := make([]string, NMap)
	for i := 0; i < NMap; i++ {
		//get file name
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceIndex)
		//try to open file if exist
		file, err := os.Open(fileName)
		if err != nil {
			//file name does not exixt
			continue
		}
		//read data asa array of key value
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	//indermediate files for Nreduce appended into kva , perform the sorting by key
	sort.Sort(ByKey(kva))

	return kva

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
