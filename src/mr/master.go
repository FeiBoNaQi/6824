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

type Master struct {
	// Your definitions here.
	state     []int8
	location  []string
	startTime []time.Time
	NReduce   int
	mutex     sync.Mutex
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

func (m *Master) Communicate(args *CommunicateArgs, reply *CommunicateReply) error {
	reply.TaskNumber = 0
	reply.Location = os.Args[1]
	reply.Task = mapTask
	reply.NReduce = m.NReduce
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
	m.NReduce = nReduce

	// Your code here.

	m.server()
	return &m
}

const (
	// const represent task states
	idle       = 0
	inProgress = 1
	completed  = 2
	timeout    = 3

	// timeout period seconds
	timeSlot = 10

	// map and reduce function definitions
	idleTask   = 0
	mapTask    = 1
	reduceTask = 2
	exitTask   = 3
)
