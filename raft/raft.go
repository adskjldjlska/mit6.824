package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// raft层向key/value层上传的信息对应start
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LeaderState = itoa
	CandidateState
	FollowerState
)

const minTimeOutInterval = 700 * time.Millisecond
const maxTimeOutInterval = 1200 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers ，代表所有其他的服务器索引
	persister *Persister          // Object to hold this peer's persisted state ,持久化状态细腻秀
	me        int                 // this peer's index into peers[]  自己在peers中的下标索引
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int   //给谁头了票
	logs        []int //日志命令条款，第一个日志的索引为1

	commitIndex int //最高的已提交日至索引
	lastApplied int // 最高的已经应用到状态机的索引

	// leader特有的易失性状态
	nextIndex  []int //
	matchIndex []int

	// 自己补充的部分就在这下面
	NoReceiveHeartBeatTimeoutMachine time.Timer //检测是否有leader发送心跳
	voteTimeoutMachine               time.Timer // 检测成为candidate后的竞选是否超时
	timeoutInterval                  int        // 以ms为单位
	state                            int
	leaderId                         int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.me == rf.leaderId
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm        int
	CandidateId          int
	CandidateLastLogidx  int // 最后一条日至的下标
	CandidateLastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if !rf.killed() {
		rf.ReSetTimeOut() // 收到投票的请求后重置自己的timeout，避免过多candidate出现
		if args.CandidateTerm < rf.currentTerm {
			reply.VoteGranted = false
		} else if args.CandidateTerm >= rf.currentTerm && args.CandidateLastLogidx >= len(rf.logs)-1 {
			reply.VoteGranted = true
			rf.currentTerm = args.CandidateTerm
		}
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

// 这里的command在test_test.go中好像是int,所以对应log的类型也应该是int吧
// key/value层向raft层发送的命令
// 只有在start函数返回后（即master接收到绝大多数相应后，知道请求已经预发到各个备机），该client请求对应的消息彩绘出现在key/value 层的ApplyChannel中
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	id := LookUpEleInSlice(rf.logs, command.(int))
	if id <= rf.commitIndex {
		index = id
	}
	term, isLeader = rf.GetState()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.NoReceiveHeartBeatTimeoutMachine.C:
			rf.state = CandidateState
			rf.currentTerm++
			numofTicket := 0
			finished := 0
			var mu sync.Mutex
			cond := sync.NewCond(&mu)
			args := RequestVoteArgs{CandidateTerm: rf.currentTerm, CandidateId: rf.me, CandidateLastLogidx: len(rf.logs) - 1}
			reply := RequestVoteReply{VoteGranted: false}
			for index, _ := range rf.peers {
				if index != rf.me {
					go func() {
						rf.sendRequestVote(index, &args, &reply)
						if reply.VoteGranted {
							numofTicket++
						}
						finished++
						cond.Broadcast()
					}()
				} else {
					numofTicket++
					finished++
				}
			}

			mu.Lock()
			for numofTicket <= len(rf.peers)/2 && finished != len(rf.peers) {
				cond.Wait()
			}
			if numofTicket > len(rf.peers)/2 {
				rf.state = LeaderState
				rf.SendAppendEntries2AllServer()
			}
		default:

		}
	}
}

func (rf *Raft) SendAppendEntries2AllServer() {
	msg := AppendEntriesMsg{LeaderId: rf.me, LeaderTerm: rf.currentTerm, Entries: nil}
	reply := ReplyForAppendEntries{Success: false}
	for index, _ := range rf.peers {
		rf.peers[index].Call("Raft.GetAppendEntries", &msg, &reply)
	}

}

func (rf *Raft) GetAppendEntries() {
	rf.ReSetTimeOut() //重置超时计时器
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FollowerState
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 生成700-1200ms范围内的超时间隔
	rand.Seed(time.Now().UnixNano())
	curInterval := time.Duration(rand.Intn(500)+700) * time.Millisecond
	rf.NoReceiveHeartBeatTimeoutMachine.Reset(curInterval)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

type HeartBeatMsg struct {
	LeaderTerm int
	LeaderId   int
	Entries    []int // emptry for heartbeat
}

type ReplyForHeartBeatMsg struct {
	Ack bool
}

type AppendEntriesMsg struct {
	LeaderTerm        int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []int // emptry for heartbeat
	LeaderCommitIndex int
}

type ReplyForAppendEntries struct {
	CurrentTerm int
	Success     bool
}

// 重置计时器
func (rf *Raft) ReSetTimeOut() {
	rand.Seed(time.Now().UnixNano())
	curInterval := time.Duration(rand.Intn(500)+700) * time.Millisecond
	if !rf.NoReceiveHeartBeatTimeoutMachine.Stop() {
		select {
		case <-rf.NoReceiveHeartBeatTimeoutMachine.C: // try to drain the channel
		default:
		}
	}
	rf.NoReceiveHeartBeatTimeoutMachine.Reset(curInterval)
}

func (rf *Raft) GetHeartBeat(args *HeartBeatMsg, reply *ReplyForHeartBeatMsg) {
	rf.ReSetTimeOut() //重置超时计时器
	rf.state = FollowerState
}

// 如果当前服务器是leader的话就向别的服务器发送心跳
func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false {
		args := HeartBeatMsg{LeaderTerm: rf.currentTerm, LeaderId: rf.me}
		reply := ReplyForHeartBeatMsg{false}
		for _, val := range rf.peers {
			val.Call("Raft.GetHeartBeat", &args, &reply)
		}
		time.Sleep(100 * time.Millisecond) //每隔100ms向别的服务器发送心跳信号
	}
}

/*填写RequestVoteArgs和RequestVoteReply结构。修改Make（）以创建一个后台goroutine，当它有一段时间没有收到另一个同行的消息时，通过发送RequestVote RPC来定期启动领导人选举。通过这种方式，
如果已经有领导者，同龄人将了解谁是领导者，或者自己成为领导者。实现RequestVote（）RPC处理程序，以便服务器相互投票。*/

/*要实现检测信号，请定义AppendEntries RPC结构（尽管您可能还不需要所有参数），并让leader定期发送它们。编写一个AppendEntries RPC处理程序方法，该方法重置选举超时，以便其他服务器在已经当选时不会作为领导者介入。*/

// test request 测试员要求你的Raft在老领导者失败后的五秒钟内选出一个新的领导者（如果大多数对等体仍能通信）。然而，请记住，如果出现分裂投票，
// 领袖选举可能需要多轮投票（如果数据包丢失或候选人不走运地选择相同的随机退避时间，就会发生这种情况）。你必须选择足够短的选举超时（以及心跳间隔），即使需要多轮选举，也很可能在5秒内完成。

/*您需要编写定期执行操作或在时间延迟后执行操作的代码。最简单的方法是创建一个goroutine，其中包含一个调用time.Sleep（）的循环；（请参阅Make（）为此目的创建的ticker（）goroutine）。
不要使用Go的时间。计时器或时间。Ticker，它们很难正确使用。*/

/*该论文的第5.2节提到了150到300毫秒的选举超时。只有当领导者发送心跳的频率远高于每150毫秒一次时，这样的范围才有意义。因为测试人员将你的心跳限制在每秒10次，
所以你必须使用比论文的150到300毫秒更大的选举超时，但不要太大，因为那样你可能无法在五秒内选出领导人。*/ //自己初步假设500-1000ms

/*测试人员在永久关闭实例时调用Raft的rf.Kill（）。您可以使用rf.killed（）检查是否调用了Kill（）。您可能希望在所有循环中都这样做，以避免死Raft实例打印出令人困惑的消息。*/

/*每个“通过”行包含五个数字；这些是测试花费的时间（以秒为单位）、Raft对等端的数量、测试期间发送的RPC数量、RPC消息中的总字节数以及提交Raft报告的日志项的数量。
如果愿意，您可以忽略这些数字，但它们可能有助于您检查实现发送的RPC数量。对于所有实验室2、3和4，如果所有测试（进行测试）的时间超过600秒，或者任何单个测试的时间超过120秒，
评分脚本将使您的解决方案失败。*/
