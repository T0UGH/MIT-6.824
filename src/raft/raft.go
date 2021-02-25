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
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const HeartBeatTime = 100 * time.Millisecond

// 当每个Raft peer意识到连续的日志条目是已提交的后，peer应发送一个ApplyMsg到位于同一个服务器上的服务,via the applyCh passed to Make()。
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer. 一个实现单个Raft peer的go对象
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers peer的标识符
	persister *Persister          // Object to hold this peer's persisted state 持久化状态
	me        int                 // this peer's index into peers[] 我是谁
	timeout   *time.Timer         //超时时钟
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久化状态
	currentTerm int
	voteFor     int
	log         []Log

	// 所有服务器上的易失性状态
	commitIndex int
	lastApplied int

	// 领导者上的易失性状态
	isLeader   bool // 当前节点是否是leader
	nextIndex  []int
	matchIndex []int
}

type Log struct {
	Command interface{}
	Term    int
}

func (rf *Raft) initState() {
	rf.isLeader = false
	rf.currentTerm = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.voteFor = -1
	rf.log = make([]Log, 0)
}

// 返回当前的term号，以及这个peer是否leader
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

// 保存持久化状态的函数
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state. 重新加载之前存储的持久化状态
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure. 请求投票RPC的参数结构体
// field names must start with capital letters! 属性名必须大写开头
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure. 请求投票RPC的回复结构体
// field names must start with capital letters! 属性名必须大写开头
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log //todo: 这里是Log类型还是Command类型,不好说啊
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// todo: 实现2B2C
}

//
// example RequestVote RPC handler. 请求投票RPC的处理函数
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 如果参数中的任期小于当前任期，一看就是个老candidate，直接返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果参数中的任期等于当前任期，那么已经投过票了，返回投票结构就行了
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = rf.voteFor == args.CandidateId
		return
	}

	// todo 检查一下这个判断语句对不对
	// 如果最后一条日志的任期比args.LastLogTerm新，或者 一样term一样新，但是len(rf.log) - 1 比 args.lastLogIndex 要大，就不投票给它
	DPrintf("raft/raft/RequestVote: compare new server [%d] compare[%+v] log[%+v]", rf.me, args, rf.log)
	if len(rf.log) > 0 && ((args.LastLogTerm < rf.log[len(rf.log)-1].Term) ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// todo: 如果我是上一轮的候选者，这个时候我可能还没有选举完
	// 如果任期大于当前任期，且候选者的日志还比较新，就给它投
	rf.currentTerm = args.Term
	rf.voteFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	return

}

//
// Append Entries Handler
// Append Entries RPC的处理函数
//
// todo: 处理并发
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// 如果term比自己还老，说明这个是个老领导，老领导的AppendEntries就不用管了，直接返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2 prevLog没有怎么处理
	noPrevLog := args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != -1 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term
	if noPrevLog {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 3and4 截取并且拼接
	DPrintf("raft/raft/AppendEntries: server [%d] update log[] from [%v]", rf.me, rf.log)
	if args.PrevLogIndex <= -1 {
		rf.log = append(rf.log[:0], args.Entries...)
	} else {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	}
	DPrintf("raft/raft/AppendEntries: server [%d] update log[] to [%v]", rf.me, rf.log)

	// 如果我是领导，并且还收到了一个至少termNumber跟我一样新的AppendEntries，就说明我是老领导，则设置我不是领导了
	if rf.isLeader {
		rf.isLeader = false
	}
	// 如果term比我还新，就说明我的term是旧的，就更新一下
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	rf.mu.Lock()
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("raft/raft/AppendEntries: if args.LeaderCommit[%d] > rf.commitIndex[%d] | len(rf.log)[%d]", args.LeaderCommit, rf.commitIndex, len(rf.log))
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// 通过信道发送已提交的命令
		for i := oldCommitIndex + 1; i < rf.commitIndex+1; i++ {
			applyMsg := ApplyMsg{
				Index:   i + 1,
				Command: rf.log[i].Command,
			}
			rf.applyCh <- applyMsg
			DPrintf("raft/raft/AppendEntries: server[%d] send to applyCh [%v]", rf.me, applyMsg)
		}
	}
	rf.mu.Unlock()

	// 重置时钟
	rf.timeout.Stop()
	rf.timeout.Reset(randVoteTime(HeartBeatTime))

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

// 发送请求投票RPC给其他server
// server参数是rf.peers[]里面对应的index号
// args就是发送的
// reply就是回复的结果
// labrpc软件包模拟了一个有损网络，在该网络中服务器可能无法访问，并且请求和回复可能会丢失。
// Call（）发送一个请求并等待答复。 如果答复在超时间隔内到达，则Call（）返回true；否则，返回true。 否则，Call（）返回false。 因此，Call（）可能会暂时不返回。
// 错误的返回可能由服务器故障，无法访问的活动服务器，请求丢失或答复丢失引起。
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
// is no need to implement your own timeouts around Call(). 不用实现自己的超时逻辑
//
// look at the comments in ../labrpc/labrpc.go for more details. 可以看看这里面
//
//
// 如果您在使RPC无法正常工作时遇到麻烦，请检查是否已大写通过RPC传递的结构中的所有字段名，并且调用方使用＆而不是结构本身传递了答复结构的地址。
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("raft/raft/sendRequestVote: from[%d] to[%d] args[%+v] reply[%+v] ok[%v]", rf.me, server, args, reply, ok)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("raft/raft/sendAppendEntries: from[%d] to[%d] args[%+v] reply[%+v] ok[%v]", rf.me, server, args, reply, ok)

	return ok
}

//
// 使用Raft的服务（例如k / v服务器）想要附加到Raft日志中的下一个命令开始达成协议。
// 如果此服务器不是领导者，则返回false。
// 否则，请启动协议并立即返回。 由于领导者可能会挂了或选举失败，因此无法保证此命令将被提交到raft日志。
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//todo:这个相当于上层在调用AppendEntries，而且还要马上返回
	if !rf.isLeader {
		return -1, -1, false
	}
	// 放到日志里面
	// todo: 加锁还是不加锁
	rf.mu.Lock()
	index := len(rf.log)
	rf.log = append(rf.log, Log{Command: command, Term: rf.currentTerm})
	term := rf.currentTerm
	isLeader := true
	// Your code here (2B).
	rf.mu.Unlock()
	return index + 1, term, isLeader
}

// 杀死时会调用，先不用管
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// 服务或tester想要创建Raft服务器。 所有Raft服务器（包括该Raft服务器）的端口都位于peers []中。 该服务器的端口是peers [me]。
// 所有服务器的peers []数组的顺序相同。 persister是该服务器保存其持久状态的位置，并且初始化时还保存最近保存的状态（如果有）。
// applyCh是tester或服务期望Raft发送ApplyMsg消息的通道.Make（）必须快速返回，因此对于任何长时间运行的工作，它应该启动goroutines。
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
	//todo: 如果commit完了要通过applyCh发送给上层app
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.initState()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.timeout = time.NewTimer(randVoteTime(HeartBeatTime))

	// 启动投票后台进程
	go voteBackground(rf)

	// 启动appendEntries后台进程
	go appendEntriesBackground(rf)

	return rf
}

const RandArgUpper int = 10
const RandArgLower int = 3

/**
* 根据传入的heartbeat来生成一个选举时间
 */
func randVoteTime(heartbeatTime time.Duration) time.Duration {
	rand.Seed(time.Now().UnixNano())
	randArg := RandArgLower + rand.Intn(RandArgUpper-RandArgLower)
	return time.Duration(randArg) * heartbeatTime
}

/**
* 监听timeout时钟信道，当时钟信道触发时就开始一次选举
 */
func voteBackground(rf *Raft) {
	for range rf.timeout.C {
		// 如果本来就是leader的话，就不用vote了
		if rf.isLeader {
			continue
		}
		rf.currentTerm += 1
		rf.voteFor = rf.me
		currentTerm := rf.currentTerm
		// 发起投票
		DPrintf("raft/raft/voteBackground: server[%d] start vote term[%d]", rf.me, currentTerm)
		var voteNum int32 = 1
		var handleVote = false
		waitGroup := sync.WaitGroup{}
		waitGroup.Add(len(rf.peers) - 1)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			lastLog := Log{Command: nil, Term: -1}
			logLen := len(rf.log)
			if logLen > 0 {
				lastLog = rf.log[logLen-1]
			}
			go func(i int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: logLen - 1,
					LastLogTerm:  lastLog.Term,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok && reply.VoteGranted {
					atomic.AddInt32(&voteNum, 1)
					rf.mu.Lock()
					if voteNum > int32(len(rf.peers)/2) && !handleVote {
						handleVote = true
						// 选举成功，那就可以开始发heartbeat了
						DPrintf("raft/raft/voteBackground: server[%d] vote success term[%d] with vote[%d/%d]", rf.me, currentTerm, voteNum, len(rf.peers))

						rf.isLeader = true
						rf.currentTerm = currentTerm
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.matchIndex {
							rf.matchIndex[i] = 0
						}
						DPrintf("raft/raft/voteBackground: init matchIndex[%v]", rf.matchIndex)
						rf.nextIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}
						DPrintf("raft/raft/voteBackground: init nextIndex[%v]", rf.nextIndex)
					}
					rf.mu.Unlock()
					DPrintf("raft/raft/voteBackground: server[%d] got vote from [%d] term[%d]", rf.me, i, currentTerm)
				}
				waitGroup.Done()
			}(i)
		}
		waitGroup.Wait()
		if !handleVote {
			// 选举失败，就设置一个随机的时钟，过一会儿再重新选举
			DPrintf("raft/raft/voteBackground: server[%d] vote fail term[%d] with vote[%d/%d]", rf.me, currentTerm, voteNum, len(rf.peers))
			rf.timeout.Reset(randVoteTime(HeartBeatTime))
		}
	}
}

/**
* 开启一个后台进程，如果rf.isLeader就定期发heartbeat，不然就不发
 */
func appendEntriesBackground(rf *Raft) {
	for {
		time.Sleep(HeartBeatTime)
		if !rf.isLeader {
			continue
		}
		logLen := len(rf.log)
		var okNum int32 = 1
		var applied bool = false
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				nextIndex := rf.nextIndex[i]
				//DPrintf("raft/raft/appendEntriesBackground: nextIndex := rf.nextIndex[%d] [%v]", i, rf.nextIndex[i])
				entries := make([]Log, 0)
				if nextIndex != -1 && nextIndex < logLen {
					entries = rf.log[nextIndex:logLen]
				}
				prevLogTerm := -1
				if nextIndex > 0 {
					prevLogTerm = rf.log[nextIndex-1].Term
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok && reply.Term > rf.currentTerm && rf.isLeader == true {
					// 先只把appendEntries停掉就行
					rf.isLeader = false
				}
				if reply.Success {
					// todo: 这里是否需要考虑并发问题 比如上一个请求还没返回回来，下一个请求已经发出并且回来了
					atomic.AddInt32(&okNum, 1)
					if rf.matchIndex[i] < logLen-1 {
						rf.matchIndex[i] = logLen - 1
						rf.nextIndex[i] = logLen
					}
				} else {
					// 如果失败了，把nextIndex向后减少一位，等下次loop再发
					if rf.nextIndex[i] > 0 {
						rf.nextIndex[i] -= 1
					}

				}
				// 加锁发消息
				rf.mu.Lock()
				if okNum > int32(len(rf.peers)/2) && !applied {
					oldCommitIndex := rf.commitIndex
					if len(rf.log) < logLen {
						rf.commitIndex = len(rf.log) - 1
					} else {
						rf.commitIndex = logLen - 1
					}
					applied = true
					for i := oldCommitIndex + 1; i < rf.commitIndex+1; i++ {
						applyMsg := ApplyMsg{
							Index:   i + 1,
							Command: rf.log[i].Command,
						}
						DPrintf("raft/raft/appendEntriesBackground: server[%d] send to applyCh [%v]", rf.me, applyMsg)
						rf.applyCh <- applyMsg
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}
