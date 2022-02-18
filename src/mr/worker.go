package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	args := CommunicateArgs{
		TaskNumber: -1,
	}
	for {
		reply := CommunicateReply{}
		call("Master.Communicate", &args, &reply)
		fmt.Printf("reply.TaskNumber %v\n", reply.TaskNumber)
		fmt.Printf("reply.Location %v\n", reply.Location)
		fmt.Printf("reply.Task %v\n", reply.Task)
		fmt.Printf("reply.NReduce %v\n", reply.NReduce)

		switch reply.Task {
		case idleTask:
			time.Sleep((1 * time.Second))
		case mapTask:
			file, err := os.Open(reply.Location)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Location)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Location)
			}
			file.Close()
			intermediate := mapf(reply.Location, string(content))
			enc := make([]*json.Encoder, reply.NReduce)
			intermediateFile := make([]*os.File, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				filename := "mr-" + fmt.Sprintf("%v", reply.TaskNumber) + "-" + fmt.Sprintf("%v", i)
				intermediateFile[i], err = os.Create(filename)
				if err != nil {
					log.Fatalf("cannot create %v", filename)
				}
				enc[i] = json.NewEncoder(intermediateFile[i])
			}

			for _, kv := range intermediate {
				reduceOrder := ihash(kv.Key) % reply.NReduce
				err := enc[reduceOrder].Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write to reduce order %v", reduceOrder)
				}
			}

			for i := 0; i < reply.NReduce; i++ {
				intermediateFile[i].Close()
			}
			args.TaskNumber = reply.TaskNumber
			args.Location = "mr-" + fmt.Sprintf("%v", reply.TaskNumber) // useless right now
		case reduceTask:
			time.Sleep(time.Second)
		case exitTask:
			return
		}

	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := CommunicateArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := CommunicateReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Communicate", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

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
