package mr

import (
	"fmt"
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
	state       []int8
	location    []string
	startTime   []time.Time
	NReduce     int
	masterState int8
	mutex       sync.Mutex
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// receive information from the workers
	if args.TaskNumber != -1 {
		m.state[args.TaskNumber] = completed
		m.location[args.TaskNumber] = args.Location
	}
	// iterate over every input file  === iterate over every map task
	taskCompleted := 0
	timeOutTask := -1
	if m.masterState == mapTask {
		for i := 0; i < len(m.state)-m.NReduce; i++ {
			if m.state[i] == idle {
				reply.TaskNumber = i
				reply.Location = os.Args[1+i]
				reply.Task = mapTask
				reply.NReduce = m.NReduce
				m.startTime[i] = time.Now()
				m.state[i] = inProgress
				return nil
			} else if m.state[i] == completed {
				taskCompleted++
			} else if m.state[i] == inProgress {
				elapsed := time.Now().Sub(m.startTime[i])
				if elapsed > timeSlot*000000000 {
					m.state[i] = timeout
					timeOutTask = i
				}
			} else if m.state[i] == timeout {
				timeOutTask = i
			}
		}
		// if no task is idle, run the timeout task
		if timeOutTask != -1 {
			reply.TaskNumber = timeOutTask
			reply.Location = os.Args[1+timeOutTask]
			reply.Task = mapTask
			reply.NReduce = m.NReduce
			m.startTime[timeOutTask] = time.Now()
			m.state[timeOutTask] = inProgress
			return nil
		}
		// if no task timeout and still some task is running, reply with idle task
		if taskCompleted != len(m.state)-m.NReduce {
			reply.Task = idleTask
			return nil
		}
		// if every map task is completed, master goes into reduce state
		m.masterState = reduceTask
	}
	// master in reduce state
	if m.masterState == reduceTask {
		taskCompleted = 0
		for i := len(m.state) - m.NReduce; i < len(m.state)-m.NReduce; i++ {
			if m.state[i] == idle {
				reply.TaskNumber = i
				reply.Location = fmt.Sprintf("%v", i-len(m.state)+m.NReduce)
				reply.Task = reduceTask
				reply.NReduce = m.NReduce
				m.startTime[i] = time.Now()
				m.state[i] = inProgress
				return nil
			} else if m.state[i] == completed {
				taskCompleted++
			} else if m.state[i] == inProgress {
				elapsed := time.Now().Sub(m.startTime[i])
				if elapsed > timeSlot*000000000 {
					m.state[i] = timeout
					timeOutTask = i
				}
			} else if m.state[i] == timeout {
				timeOutTask = i
			}
			// if no task is idle, run the timeout task
			if timeOutTask != -1 {
				reply.TaskNumber = timeOutTask
				reply.Location = fmt.Sprintf("%v", timeOutTask-len(m.state)+m.NReduce)
				reply.Task = reduceTask
				reply.NReduce = m.NReduce
				m.startTime[timeOutTask] = time.Now()
				m.state[timeOutTask] = inProgress
				return nil
			}
			// if no task timeout and still some task is running, reply with idle task
			if taskCompleted != len(m.state)-m.NReduce {
				reply.Task = idleTask
				return nil
			}
			m.masterState = exitTask
		}
	}
	if m.masterState == exitTask {
		reply.Task = exitTask
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
	m.NReduce = nReduce
	m.state = make([]int8, nReduce+len(os.Args)-1)
	m.location = make([]string, nReduce+len(os.Args)-1)
	m.startTime = make([]time.Time, nReduce+len(os.Args)-1)
	m.masterState = mapTask
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
