package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerInfo struct {
	FinishedTask_ bool
}

type Tasks struct {
	Ismap           bool
	Mapfilename_    string //map任务处理的文件
	Reducefilename_ string //reduce 人物处理的文件
	Mapid_          int
	Reduceid_       int
	ReduceNum_      int
	FielsNum_       int
	IsEffective_    bool //判断任务是否有效,主要用于超时任务的判断
}

type MapReply struct {
	task_       Tasks
	MapTempFile []*os.File
}

type ReduceReply struct {
	task_          Tasks
	ReduceTempFile *os.File
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
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

// main/mrworker.go calls this function.
//

// mapf参数为文件名，文件内容,reducef参数为key(string),存储对应key值的所有value的切片
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	/*sockname := coordinatorSock()
	c, err3 := rpc.DialHTTP("unix", sockname)
	if err3 != nil {
		log.Fatal("dialing:", err3)
	}
	defer c.Close()*/
	var args WorkerInfo
	args.FinishedTask_ = false
	for {

		// Your worker implementation here.
		// declare a reply structure.
		reply := Tasks{}

		err := call("Coordinator.Communicate", &args, &reply)
		if !err {
			fmt.Println("向master索要任务不成功退出!")
			return //打不通说明所有任务已经完成了，coordinator已经退出了
		}
		if !reply.IsEffective_ {
			time.Sleep(1 * time.Second)
			continue
		}

		if reply.Ismap {
			file, err := os.Open(reply.Mapfilename_)
			if err != nil {
				fmt.Printf("because:%v\n", err)
				log.Fatalf("cannot open %v", reply.Mapfilename_)

			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				fmt.Printf("because:%v\n", err)
				log.Fatalf("cannot read %v", reply.Mapfilename_)

			}
			file.Close()
			kva := mapf(reply.Mapfilename_, string(content))
			intermediateFiles := make([][]KeyValue, reply.ReduceNum_)
			for _, val := range kva {
				reduce_task_number := ihash(val.Key) % reply.ReduceNum_
				intermediateFiles[reduce_task_number] = append(intermediateFiles[reduce_task_number], val)

			}
			var mapTempFile []*os.File
			for index, val := range intermediateFiles {
				intermediate_file := "mr-" + strconv.Itoa(reply.Mapid_) + "-" + strconv.Itoa(index)
				curfile, err := os.Create(intermediate_file)
				if err != nil {
					fmt.Printf("打开文件：%v出错\n", intermediate_file)
				}
				/*dir, _ := os.Getwd()
				curtempfile, _ := ioutil.TempFile(dir, "mr-Maptmp-*")*/
				//mapTempFile = append(mapTempFile, curtempfile)
				enc := json.NewEncoder(curfile)
				for _, kv := range val {
					err := enc.Encode(&kv)
					if err != nil {
						fmt.Printf("because:%v\n", err)
						fmt.Printf("Json编码时出错!\n")

					}
				}
				curfile.Close()
			}

			err1 := call("Coordinator.PutFinishedMapTask", &reply, &args)
			if !err1 {
				fmt.Printf("because:%v\n", err1)
				fmt.Printf("将完成的map任务提交时出错\n")
				for _, val := range mapTempFile {
					os.Remove(val.Name())
				}
				return //打不通说明所有任务已经完成了，coordinator已经退出了
			}

		} else {
			var kva []KeyValue
			for i := 0; i < reply.FielsNum_; i++ {
				curFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Reduceid_)
				file, err := os.Open(curFileName)
				if err != nil {
					fmt.Printf("because:%v\n", err)
					log.Fatalf("cannot open map任务产生的文件%v", reply.Mapfilename_)

				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))

			dir, _ := os.Getwd()
			//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
			tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
			if err != nil {
				fmt.Printf("because:%v\n", err)
				log.Fatal("Failed to create temp file", err)

			}

			/*oname := "mr-out-" + strconv.Itoa(reply.Reduceid_)
			ofile, _ := os.Create(oname)*/

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			tempFile.Close()
			err1 := call("Coordinator.PutFinishedReduceTask", &reply, &args)
			if !err1 {
				fmt.Printf("将完成的reduce任务提交时出错\n")
				fmt.Printf("because:%v\n", err1)
				os.Remove(tempFile.Name())
				return //打不通说明所有任务已经完成了，coordinator已经退出了
			}
			if args.FinishedTask_ {
				os.Rename(tempFile.Name(), "mr-out-"+strconv.Itoa(reply.Reduceid_))
			} else {
				os.Remove(tempFile.Name())
			}

		}

	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

// worker向coordinator通信的例子
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
