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

type TaskType int
type TaskStatus int

const maxExecTime = 10
const tmpDir = "./tmp"

const (
	Map TaskType = iota
	Reduce
	NoIdleTask
	Exit
)

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	TaskId   int
	FilePath string
	WorkerId int
}

type Master struct {
	// Your definitions here.
	mu           sync.Mutex
	mapTasks     []Task
	reduceTasks  []Task
	nMap         int
	nReduce      int
	nextWorkerId int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestWorkerId(args *RequestWorkerIdArgs, reply *RequestWorkerIdReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.WorkerId = m.nextWorkerId
	m.nextWorkerId = m.nextWorkerId + 1

	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// get a idle task then make worker exec it
	m.mu.Lock()
	task := m.GetIdleTask()
	task.Status = InProgress
	task.WorkerId = args.WorkerId
	reply.Type = task.Type
	reply.TaskId = task.TaskId
	reply.FilePath = task.FilePath
	m.mu.Unlock()

	// set timeout
	go m.setTimeout(task)

	return nil
}

func (m *Master) setTimeout(task *Task) {
	// If the task is still in-progress after the max_exec_time, the master reset the task to idle (therefore, can be schedule to another worker)

	// <-time.After(time.Second * maxExecTime)
	// m.mu.Lock()
	// defer m.mu.Unlock()
	// if task.Status == InProgress && (task.Type == Map || task.Type == Reduce) {
	// 	task.Status = Idle
	// 	fmt.Printf("GG, type:%d id:%d worker: %d\n", task.Type, task.TaskId, task.WorkerId)
	// }

	if task.Type == Map || task.Type == Reduce {
		<-time.After(time.Second * maxExecTime)
		m.mu.Lock()
		defer m.mu.Unlock()
		if task.Status == InProgress {
			task.Status = Idle
			fmt.Printf("worker failure, workerId: %d taskType:%d taskId:%d \n", task.WorkerId, task.Type, task.TaskId)
		}
	}
}

func (m *Master) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.ReduceCount = len(m.reduceTasks)
	return nil
}

func (m *Master) GetIdleTask() *Task {
	// order of task assignment:
	// 	(1) map tasks
	// 	(2) reduce tasks
	// 	(3) exit

	// select type
	var tasks []Task
	if m.nMap > 0 {
		tasks = m.mapTasks
	} else if m.nReduce > 0 {
		tasks = m.reduceTasks
	} else {
		return &Task{Exit, Completed, -1, "", -1}
	}

	// iterate through tasks and find an idle one
	for i := range tasks {
		task := &tasks[i]
		if task.Status == Idle {
			return task
		}
	}

	// have no idle task but some in-progress tasks
	return &Task{NoIdleTask, Completed, -1, "", -1}
}
func (m *Master) showUnfinishedTasks(taskType TaskType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var tasks []Task
	if taskType == Map {
		tasks = m.mapTasks
		fmt.Printf("%d remaining map    tasks: ", m.nMap)
	} else if taskType == Reduce {
		tasks = m.reduceTasks
		fmt.Printf("%d remaining reduce tasks: ", m.nReduce)
	}

	for i := range tasks {
		task := &tasks[i]
		if task.Status != Completed {
			fmt.Printf("%d ", task.TaskId)
		}
	}
	fmt.Printf("\n")
}

func (m *Master) ReportCompleted(args *ReportCompletedArgs, reply *ReportCompletedReply) error {
	m.mu.Lock()
	var tasks []Task
	if args.Type == Map {
		tasks = m.mapTasks
	} else if args.Type == Reduce {
		tasks = m.reduceTasks
	} else {
		log.Fatalf("wrong task type", args.Type)
		return nil
	}

	task := &tasks[args.TaskId]
	if task.WorkerId == args.WorkerId {
		task.Status = Completed
		if args.Type == Map {
			m.nMap -= 1
		} else {
			m.nReduce -= 1
		}
		fmt.Printf("worker %v successfully finished %d task %v\n", task.WorkerId, task.Type, args.TaskId)
		//fmt.Printf("map tasks : %v\n\n", m.mapTasks)
	} else {
		log.Fatalf("workerId expired ( expect %v but get %v)", task.WorkerId, args.WorkerId)
	}
	m.mu.Unlock()

	m.showUnfinishedTasks(Map)
	m.showUnfinishedTasks(Reduce)
	fmt.Println("")
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
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.nMap == 0 && m.nReduce == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	//step1. initialize the parameters and tmp dir
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTasks = make([]Task, 0, m.nMap)
	m.reduceTasks = make([]Task, 0, m.nReduce)
	m.nextWorkerId = 0
	fmt.Println(m.nMap)

	err := os.RemoveAll(tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.Mkdir(tmpDir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	//step2. initialize map & reduce tasks
	for i, filePath := range files {
		t := Task{}
		t.Type = Map
		t.Status = Idle
		t.TaskId = i
		t.FilePath = filePath
		t.WorkerId = -1
		fmt.Printf("%+v\n", t)
		m.mapTasks = append(m.mapTasks, t)
	}
	for i := 0; i < m.nReduce; i = i + 1 {
		t := Task{}
		t.Type = Reduce
		t.Status = Idle
		t.TaskId = i
		t.FilePath = ""
		t.WorkerId = -1
		fmt.Printf("%+v\n", t)
		m.reduceTasks = append(m.reduceTasks, t)
	}

	//step3  run server
	m.server()

	return &m
}
