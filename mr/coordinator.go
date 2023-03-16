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

var (
	TasksChannel       chan Tasks
	MapTaskFinished    chan Tasks
	ReduceTasksChannel chan Tasks
	ReduceTaskFinished chan Tasks
)

type Coordinator struct {
	// Your definitions here.
	reduce_num_ int
	//files_      []string
	hadReduce bool
	filesNum_ int
}

var SucceedTasks RWMap

var TimeoutTasks RWMap

type RWMap struct { // 一个读写锁保护的线程安全的map
	sync.RWMutex // 读写锁保护下面的map字段
	m            map[Tasks]*WorkerInfo
}

// 新建一个RWMap
func NewRWMap(n int) RWMap {
	return RWMap{
		m: make(map[Tasks]*WorkerInfo, n),
	}
}
func (m *RWMap) Get(k *Tasks) (*WorkerInfo, bool) { //从map中读取一个值
	m.RLock()
	defer m.RUnlock()
	v, existed := m.m[*k] // 在锁的保护下从map中读取
	return v, existed
}

func (m *RWMap) Set(k *Tasks, v *WorkerInfo) { // 设置一个键值对
	m.Lock() // 锁保护
	defer m.Unlock()
	m.m[*k] = v
}

func (m *RWMap) Delete(k *Tasks) { //删除一个键
	m.Lock() // 锁保护
	defer m.Unlock()
	delete(m.m, *k)
}

func (c *Coordinator) CheckTimeout(id *WorkerInfo, reply *Tasks) {
	<-time.After(10 * time.Second)
	fmt.Printf("超时检测器等待了第%d号Map任务10秒\n", reply.Mapid_)
	_, ok := SucceedTasks.Get(reply)
	if !ok {
		TimeoutTasks.Set(reply, id)
		if reply.Ismap {
			TasksChannel <- *reply
			fmt.Printf("因为超时将第%d号map任务重新放回\n", reply.Mapid_)
		} else {
			ReduceTasksChannel <- *reply
			fmt.Printf("因为超时将第%d号reduce任务重新放回\n", reply.Reduceid_)
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
// 在这里服务器给worker分配任务
func (c *Coordinator) Communicate(id *WorkerInfo, reply *Tasks) error {
	select {
	case *reply = <-ReduceTasksChannel:
		go c.CheckTimeout(id, reply)
	case *reply = <-TasksChannel:
		go c.CheckTimeout(id, reply)
	default:
		if len(MapTaskFinished) == c.filesNum_ {
			mutex := sync.Mutex{}
			mutex.Lock()
			if !c.hadReduce {
				for i := 0; i < c.reduce_num_; i++ {
					ReduceTasksChannel <- Tasks{false, "", "", 0, i, c.reduce_num_, c.filesNum_, true}
				}
				c.hadReduce = true
			}
			mutex.Unlock()
		}
		reply.IsEffective_ = false
	}
	return nil
}

func (c *Coordinator) PutFinishedMapTask(reply *Tasks, id *WorkerInfo) error {
	existId, ok := TimeoutTasks.Get(reply)
	if !ok {
		SucceedTasks.Set(reply, id)
		MapTaskFinished <- *reply
		(*id).FinishedTask_ = true
		fmt.Printf("第%d号Map任务已经成功提交,由于没有超时\n", reply.Mapid_)
	} else {
		if existId != id {
			SucceedTasks.Set(reply, id)
			TimeoutTasks.Delete(reply)
			MapTaskFinished <- *reply
			(*id).FinishedTask_ = true
			fmt.Printf("第%d号Map任务已经成功提交,由于超时但是被新的worker按时完成\n", reply.Mapid_)
		} else {
			TimeoutTasks.Delete(reply)
			(*id).FinishedTask_ = false
			fmt.Printf("第%d号Map任务因超时但执行完毕被舍弃\n", reply.Mapid_)
		}

	}

	return nil
}

func (c *Coordinator) PutFinishedReduceTask(reply *Tasks, id *WorkerInfo) error {
	existId, ok := TimeoutTasks.Get(reply)
	if !ok {
		SucceedTasks.Set(reply, id)
		ReduceTaskFinished <- *reply
		(*id).FinishedTask_ = true
		fmt.Printf("第%d号reduce任务已经成功提交,由于没有超时\n", reply.Reduceid_)
	} else {
		if existId != id {
			SucceedTasks.Set(reply, id)
			TimeoutTasks.Delete(reply)
			ReduceTaskFinished <- *reply
			(*id).FinishedTask_ = true
			fmt.Printf("第%d号reduce任务已经成功提交,由于超时但是被新的worker按时完成\n", reply.Reduceid_)
		} else {
			TimeoutTasks.Delete(reply)
			(*id).FinishedTask_ = false
			fmt.Printf("第%d号reduce任务因超时但执行完毕被舍弃\n", reply.Reduceid_)
		}
	}
	return nil
}

/*func (c *Coordinator) PutFailedMapTask(id *WorkerInfo, reply *Tasks) error {
	TasksChannel <- *reply

	SucceedTasks[*reply] = id

	return nil
}

func (c *Coordinator) PutFailedReduceTask(id *WorkerInfo, reply *Tasks) error {
	ReduceTasksChannel <- *reply
	mutex := sync.Mutex{}
	mutex.Lock()
	SucceedTasks[*reply] = id
	mutex.Unlock()
	return nil
}*/

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// 处理http请求好像不需要for循环一直监听
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	if len(ReduceTaskFinished) == c.reduce_num_ {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.reduce_num_ = nReduce
	c.filesNum_ = len(files)
	c.hadReduce = false
	TasksChannel = make(chan Tasks, len(files))
	MapTaskFinished = make(chan Tasks, len(files))
	ReduceTasksChannel = make(chan Tasks, nReduce)
	ReduceTaskFinished = make(chan Tasks, nReduce)
	SucceedTasks = NewRWMap(c.filesNum_ + c.reduce_num_)
	TimeoutTasks = NewRWMap(c.filesNum_ + c.reduce_num_)
	//SucceedTasks = make(map[Tasks]*WorkerInfo, int(math.Max(float64(c.filesNum_), float64(c.reduce_num_))))
	for i := 0; i < len(files); i++ {
		TasksChannel <- Tasks{true, files[i], "", i, 0, nReduce, c.filesNum_, true}
	}

	c.server()
	return &c
}
