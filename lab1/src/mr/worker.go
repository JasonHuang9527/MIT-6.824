package mr

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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

func check(e error) {
	if e != nil {
		panic(e)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// get a worker id
	workerId, succ := requestWorkerId()
	if succ == false {
		fmt.Println("Failed to request a workerId")
		return
	}
	fmt.Println("workerId:", workerId)

	// request a task repeatly until there is no task
	for {
		// request a task
		taskType, taskFilePath, taskId, succ := requestTask(workerId)
		if succ == false {
			fmt.Println("Failed to request a task")
			return
		}
		fmt.Printf("worker %d request a task => type: %d, file name:%s, task id:%d\n", workerId, taskType, taskFilePath, taskId)

		// execute the task
		if taskType == Map {
			fmt.Printf("worker %d start a map    task %d\n\n", workerId, taskId)
			execMap(mapf, taskFilePath, taskId, workerId)
			reportCompleted(Map, taskId, workerId)
		} else if taskType == Reduce {
			fmt.Printf("worker %d start a reduce task %d\n\n", workerId, taskId)
			execReduce(reducef, taskId, workerId)
			reportCompleted(Reduce, taskId, workerId)
		} else if taskType == NoIdleTask {
			fmt.Println("there is no idle task => wait")
			<-time.After(time.Second)
		} else if taskType == Exit {
			fmt.Println("exit")
			return
		} else {
			panic("unknown tasktype")
		}
	}
}

func requestWorkerId() (int, bool) {
	args := RequestWorkerIdArgs{}
	reply := RequestWorkerIdReply{}
	succ := call("Master.RequestWorkerId", &args, &reply)
	return reply.WorkerId, succ
}

func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	succ := call("Master.GetReduceCount", &args, &reply)
	return reply.ReduceCount, succ
}

func requestTask(workerId int) (TaskType, string, int, bool) {
	args := RequestTaskArgs{workerId}
	reply := RequestTaskReply{}
	succ := call("Master.RequestTask", &args, &reply)
	return reply.Type, reply.FilePath, reply.TaskId, succ

}

func reportCompleted(taskType TaskType, taskId int, workerId int) {
	args := ReportCompletedArgs{taskType, taskId, workerId}
	reply := ReportCompletedReply{}
	succ := call("Master.ReportCompleted", &args, &reply)
	if succ == false {
		fmt.Println("Failed to report that the task have been finished")
		return
	}
}

func execMap(mapf func(string, string) []KeyValue, filePath string, mapId int, workerId int) {
	nReduce, succ := getReduceCount()
	if succ == false {
		fmt.Println("Failed to get number of reduceTask")
		return
	}

	//// step1. read file content
	inpuyFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(inpuyFile)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	inpuyFile.Close()

	//// step2. call map function
	kva := mapf(filePath, string(content))

	//// step3. write results(json) to tmp files
	// split all json objs into nReduce groups
	buckets := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		bucketPath := fmt.Sprintf("%v/mr-%v-%v-%v", tmpDir, mapId, reduceId, workerId)

		bucket, err := os.Create(bucketPath)
		if err != nil {
			log.Fatalf("cannot create %v", bucketPath)
		}
		buffer := bufio.NewWriter(bucket)
		encoder := json.NewEncoder(buffer)

		buckets = append(buckets, bucket)
		buffers = append(buffers, buffer)
		encoders = append(encoders, encoder)
	}
	// assign key-value pairs to buckets (still in buffers) by the hash function
	for _, kv := range kva {
		bucketId := ihash(kv.Key) % nReduce
		err := encoders[bucketId].Encode(&kv)
		if err != nil {
			log.Fatalf("error when encoding json %v", kv)
		}
	}

	// flush the buffers
	for _, buffer := range buffers {
		err := buffer.Flush()
		if err != nil {
			log.Fatalf("error when flushing the buffer")
		}
	}

	// update the file_names (remove the worker id)
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		Original_Path := fmt.Sprintf("%v/mr-%v-%v-%v", tmpDir, mapId, reduceId, workerId)
		New_Path := fmt.Sprintf("%v/mr-%v-%v", tmpDir, mapId, reduceId)
		err := os.Rename(Original_Path, New_Path)
		if err != nil {
			log.Fatalf("error when updating %v to %v", Original_Path, New_Path)
		}
	}
}

func execReduce(reducef func(string, []string) string, reduceId int, workerId int) {
	//// step1. grab tmp files (output of mapf) by reduceId
	relatedFiles, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", tmpDir, "*", reduceId))
	if err != nil {
		log.Fatalf("error when glob files")
	}

	//// step2. create output files
	outfilePath := fmt.Sprintf("%v/mr-out-%v-%v", tmpDir, reduceId, workerId)
	outfile, err := os.Create(outfilePath)
	if err != nil {
		log.Fatalf("error when createing %v", outfilePath)
	}
	//// step3. call reducef and store the results into files
	// aggregate values with the same key into an array => key-values
	kvs := make(map[string][]string)
	var kv KeyValue
	for _, filePath := range relatedFiles {
		infile, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("error when opening %v", filePath)
		}
		dec := json.NewDecoder(infile)
		for {
			err := dec.Decode(&kv)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Fatal(err)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		infile.Close()
	}

	// sort key-values pair by key
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// call reduced for each key-values pair and write the results into file
	for _, k := range keys {
		reducefResults := reducef(k, kvs[k])
		_, err := fmt.Fprintf(outfile, "%v %v\n", k, reducefResults)
		if err != nil {
			log.Fatalf("error when writing results(reduce) to the file")
		}
	}
	outfile.Close()

	// update the file name
	Original_Path := fmt.Sprintf("%v/mr-out-%v-%v", tmpDir, reduceId, workerId)
	New_Path := fmt.Sprintf("./mr-out-%v", reduceId)
	err = os.Rename(Original_Path, New_Path)
	if err != nil {
		log.Fatalf("error when updating %v to %v", Original_Path, New_Path)
	}
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
