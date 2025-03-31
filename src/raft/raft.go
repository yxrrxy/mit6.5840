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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	state         NodeState
	leaderId      int
	lastHeartbeat time.Time
	log           []LogEntry
	commitIndex   int
	lastApplied   int

	applyCh         chan ApplyMsg
	applyLogsActive bool // 控制是否已经启动应用日志的协程

	// 领导者需要的状态
	nextIndex  []int // 对于每个服务器，发送到该服务器的下一个日志条目的索引
	matchIndex []int // 对于每个服务器，已知的已复制到该服务器的最高日志条目

	// 快照相关字段
	lastIncludedIndex int // 快照包含的最后一个日志条目的索引
	lastIncludedTerm  int // 该条目的任期

	applyCond *sync.Cond // Added for applyCond
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastApplied)
	e.Encode(rf.commitIndex)
	raftstate := w.Bytes()

	// 保持现有快照不变
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	var lastApplied int
	var commitIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// 处理必要字段的错误
		return
	}

	// 尝试解码快照字段和新增的commitIndex字段，但允许它们可能不存在
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&commitIndex) != nil {
		lastIncludedIndex = 0
		lastIncludedTerm = 0
		lastApplied = 0
		commitIndex = 0
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastApplied
	rf.commitIndex = commitIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果快照索引小于等于已包含在快照中的最后一个日志条目的索引，忽略此快照
	if index <= rf.lastIncludedIndex {
		return
	}

	// 先应用任何未应用但已提交的日志
	if rf.lastApplied < index && rf.lastApplied < rf.commitIndex {
		rf.applyLogs()
	}

	// 检查索引是否在日志范围内
	relativeIndex := index - rf.lastIncludedIndex
	if relativeIndex <= 0 || relativeIndex >= len(rf.log) {
		return
	}

	// 获取对应索引的任期
	newLastIncludedTerm := rf.log[relativeIndex].Term

	// 创建新的日志，保留索引之后的条目
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Term: newLastIncludedTerm}) // 添加哨兵条目
	newLog = append(newLog, rf.log[relativeIndex+1:]...)         // 添加索引之后的条目

	// 更新lastIncludedIndex和lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = newLastIncludedTerm

	// 更新日志
	rf.log = newLog

	// 如果lastApplied和commitIndex小于index，更新它们
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
	}

	// 持久化状态和快照
	rf.persistStateAndSnapshot(snapshot)
}

// 持久化状态和快照
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastApplied)
	e.Encode(rf.commitIndex)
	raftstate := w.Bytes()

	// 确保snapshot不为nil
	if snapshot == nil {
		snapshot = []byte{} // 使用空数组而非nil
	}

	rf.persister.Save(raftstate, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 如果收到的任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 如果收到更高的任期，更新自己的任期并转为Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist() // 持久化状态变更
	}

	// 计算自己的最后一个日志索引和任期
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1

	// 确定最后一个日志条目的任期
	var lastLogTerm int
	if len(rf.log) > 0 {
		// 如果日志不为空，使用最后一个条目的任期
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		// 如果日志为空，使用快照中的任期
		lastLogTerm = rf.lastIncludedTerm
	}

	// 日志比较规则：
	// 1. 如果任期不同，则更高任期的日志更新
	// 2. 如果任期相同，则更长的日志更新
	logIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		// 候选人的最后一个日志任期更高
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		// 任期相同但候选人的日志更长或相同
		logIsUpToDate = true
	}

	// 检查是否可以投票（未投票或已投票给相同候选人）并且候选人的日志至少与自己一样新
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logIsUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now() // 重置选举超时
		rf.persist()                  // 持久化状态变更
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是Leader，返回失败
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// 添加新的日志条目
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)

	// 计算绝对索引（考虑快照）
	index := rf.lastIncludedIndex + len(rf.log) - 1

	// 持久化状态，因为日志发生了变化
	rf.persist()

	// 更新自己的matchIndex
	rf.matchIndex[rf.me] = index

	// 立即启动日志复制过程
	go rf.sendHeartbeats()

	return index, rf.currentTerm, true
}

func (rf *Raft) checkCommit() {
	// 确保仅Leader调用这个方法
	if rf.state != Leader {
		return
	}

	// 从最大的日志索引开始检查，向前查找可以提交的位置
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1

	// 只检查未提交的条目
	for i := lastLogIndex; i > rf.commitIndex; i-- {
		// 计算相对索引
		relativeIndex := i - rf.lastIncludedIndex

		// 确保索引有效
		if relativeIndex <= 0 || relativeIndex >= len(rf.log) {
			continue
		}

		// Leader只能提交当前任期的日志
		if rf.log[relativeIndex].Term != rf.currentTerm {
			continue
		}

		// 统计已复制此日志的服务器数量
		count := 1 // 包括Leader自己
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= i {
				count++
			}
		}

		// 如果大多数服务器已复制此日志，则提交它
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			// 条件变量通知日志应用协程有新日志需要应用
			rf.applyCond.Signal()
			// 一旦找到可以提交的日志，立即返回
			return
		}
	}
}

// applyLogs持续检查并应用已提交但未应用的日志条目
func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.mu.Lock()

		// 使用条件变量或轮询检查是否有新日志需要应用
		for rf.lastApplied >= rf.commitIndex {
			// 如果没有新日志，暂时释放锁并休眠
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()

			// 如果被杀死，退出循环
			if rf.killed() {
				rf.mu.Unlock()
				return
			}

			// 再次检查条件
			if rf.lastApplied < rf.commitIndex {
				break
			}
		}

		// 如果lastApplied仍然小于commitIndex，应用日志
		if rf.lastApplied < rf.commitIndex {
			// 将需要应用的日志条目复制到一个临时切片中
			var msgsToApply []ApplyMsg

			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				// 如果索引小于最后包含的索引，跳过
				if i < rf.lastIncludedIndex {
					rf.lastApplied = rf.lastIncludedIndex
					continue
				}

				// 计算日志在当前日志数组中的相对位置
				relativeIndex := i - rf.lastIncludedIndex

				// 确保索引有效且命令不为空
				if relativeIndex > 0 && relativeIndex < len(rf.log) {
					if rf.log[relativeIndex].Command != nil {
						msg := ApplyMsg{
							CommandValid: true,
							Command:      rf.log[relativeIndex].Command,
							CommandIndex: i, // 使用绝对索引
						}
						msgsToApply = append(msgsToApply, msg)
					}
				}

				// 递增lastApplied
				rf.lastApplied = i
			}

			// 持久化当前状态，因为lastApplied已更新
			rf.persist()

			// 获取applyCh的本地引用
			applyCh := rf.applyCh

			// 释放锁，在锁外发送消息到通道
			rf.mu.Unlock()

			// 发送所有准备好的消息
			for _, msg := range msgsToApply {
				applyCh <- msg
			}
		} else {
			// 使用条件变量等待新的提交
			rf.applyCond.Wait()
			rf.mu.Unlock()
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func getRandomTimeout() time.Duration {
	//300
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			// Leader需要定期发送心跳
			if time.Since(rf.lastHeartbeat) >= 100*time.Millisecond {
				rf.lastHeartbeat = time.Now()
				go rf.sendHeartbeats() // 异步发送心跳
			}
		} else if time.Since(rf.lastHeartbeat) > getRandomTimeout() {
			// 如果是Follower或Candidate且超时，开始选举
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist() // 持久化状态变更

	// 获取自己最后一条日志的索引和任期（考虑快照）
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state == Candidate && rf.currentTerm == args.Term {
						if reply.VoteGranted {
							votes++
							if votes > len(rf.peers)/2 {
								rf.becomeLeader()
							}
						} else if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.persist() // 持久化状态变更
						}
					}
				}
			}(i)
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // 领导者的任期
	LeaderId     int        // 领导者ID
	PrevLogIndex int        // 新日志条目之前的日志索引
	PrevLogTerm  int        // PrevLogIndex条目的任期
	Entries      []LogEntry // 需要存储的日志条目（心跳为空）
	LeaderCommit int        // 领导者的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，用于领导者更新自己
	Success bool // 如果follower包含匹配prevLogIndex和prevLogTerm的日志则为true
	// 用于快速回退
	ConflictIndex int // 冲突的日志索引
	ConflictTerm  int // 冲突的日志任期
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化回复
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// 如果接收到的任期小于当前任期，拒绝请求
	if args.Term < rf.currentTerm {
		return
	}

	// 如果接收到更高任期，更新当前任期并转为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist() // 持久化状态变更
	}

	// 重置选举超时
	rf.lastHeartbeat = time.Now()

	// 如果不是Follower，变为Follower
	if rf.state != Follower {
		rf.state = Follower
	}

	// 确保日志数组已初始化
	if len(rf.log) == 0 {
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{Term: 0} // 哨兵条目
		rf.persist()
	}

	// 如果PrevLogIndex位于快照中，需要处理特殊情况
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// 请求发送者发送快照，因为我们无法验证这个PrevLogIndex
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// 计算PrevLogIndex对应的相对索引
	relativeIndex := args.PrevLogIndex - rf.lastIncludedIndex

	// 检查是否有足够的日志条目
	if relativeIndex >= len(rf.log) {
		// 日志太短，请求者需要发送更早的日志
		reply.ConflictIndex = len(rf.log) + rf.lastIncludedIndex
		reply.ConflictTerm = -1
		return
	}

	// 检查PrevLogIndex处的任期是否匹配
	if args.PrevLogIndex > 0 && rf.log[relativeIndex].Term != args.PrevLogTerm {
		// 任期不匹配，需要找到冲突点
		reply.ConflictTerm = rf.log[relativeIndex].Term

		// 找到具有此冲突任期的第一个日志条目
		var i int
		for i = relativeIndex; i > 0; i-- {
			if rf.log[i-1].Term != reply.ConflictTerm {
				break
			}
		}

		// 返回此任期的第一个条目的索引
		reply.ConflictIndex = i + rf.lastIncludedIndex
		return
	}

	// 日志匹配成功，可以接受新的日志条目
	reply.Success = true

	// 如果有新的日志条目，进行处理
	if len(args.Entries) > 0 {
		// 从PrevLogIndex之后开始追加
		nextIndex := relativeIndex + 1

		// 检查每个新条目
		for i, entry := range args.Entries {
			if nextIndex+i < len(rf.log) {
				// 如果已有条目任期不同，删除此条目及之后的所有条目
				if rf.log[nextIndex+i].Term != entry.Term {
					rf.log = rf.log[:nextIndex+i]
					rf.log = append(rf.log, entry)
				}
				// 否则保留现有条目（任期相同）
			} else {
				// 追加新条目
				rf.log = append(rf.log, entry)
			}
		}

		// 持久化状态，因为日志改变了
		rf.persist()
	}

	// 检查是否需要更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// 计算新的commitIndex（不能超过最后一个日志的索引）
		lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
		newCommitIndex := min(args.LeaderCommit, lastLogIndex)

		if newCommitIndex > rf.commitIndex {
			// 更新commitIndex
			rf.commitIndex = newCommitIndex

			// 通知日志应用协程
			rf.applyCond.Signal()
		}
	}
}

// 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		applyCh:           applyCh,
		state:             Follower,
		currentTerm:       0,
		votedFor:          -1,
		lastHeartbeat:     time.Now(),
		log:               make([]LogEntry, 1), // 从索引1开始，0是哨兵
		commitIndex:       0,                   // 初始时没有提交任何日志
		lastApplied:       0,                   // 初始时没有应用任何日志
		nextIndex:         nil,                 // 只有Leader才需要初始化这些
		matchIndex:        nil,                 // 只有Leader才需要初始化这些
		applyLogsActive:   false,               // 初始时未启动日志应用协程
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}

	// 初始化条件变量
	rf.applyCond = sync.NewCond(&rf.mu)

	// 索引0是不使用的哨兵条目，Term为0
	rf.log[0] = LogEntry{Term: 0}

	// 从持久化存储中恢复状态
	rf.readPersist(persister.ReadRaftState())

	// 如果有快照，确保commitIndex和lastApplied至少为lastIncludedIndex
	if rf.lastIncludedIndex > 0 {
		rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	}

	// 如果有快照，应用快照
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 && rf.lastIncludedIndex > 0 {
		// 确保lastApplied值正确
		rf.lastApplied = rf.lastIncludedIndex

		// 通知上层服务应用快照
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}

		// 使用新的goroutine来发送快照，避免阻塞Make函数
		go func() {
			applyCh <- msg
		}()
	}

	// 启动周期性检查选举超时的goroutine
	go rf.ticker()

	// 启动日志应用协程
	go rf.applyLogs()

	return rf
}

// 新增max函数，与min函数对应
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 成为Leader时初始化状态
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.applyLogsActive = false // 确保当成为新Leader时重置此标志

	// 初始化nextIndex和matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 计算最新的日志索引（考虑快照）
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1

	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1 // 初始化为最新日志索引+1
		rf.matchIndex[i] = 0               // 初始时假设没有匹配
	}

	// 设置Leader自己的matchIndex
	rf.matchIndex[rf.me] = lastLogIndex

	// 记录成为Leader的时间
	rf.lastHeartbeat = time.Now()

	// 立即发送心跳以建立权威
	rf.persist() // 持久化当前状态

	// 启动心跳发送
	go rf.sendHeartbeats()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC的回复结构
type InstallSnapshotReply struct {
	Term int
}

// 发送InstallSnapshot RPC的帮助函数
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	// 设置回复的任期
	reply.Term = rf.currentTerm

	// 如果leader任期小于当前任期，拒绝请求
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// 如果收到更高任期，更新当前任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// 重置选举计时器
	rf.lastHeartbeat = time.Now()

	// 确保状态是Follower
	rf.state = Follower

	// 如果快照不比当前状态新，或者快照索引小于已提交的索引，忽略此快照
	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	// 准备新的日志数组，从哨兵条目开始
	var newLog []LogEntry

	// 获取当前最大日志索引
	maxLocalLogIndex := rf.lastIncludedIndex + len(rf.log) - 1

	if maxLocalLogIndex <= args.LastIncludedIndex {
		// 如果现有日志都被快照覆盖，创建一个只包含哨兵条目的新日志
		newLog = []LogEntry{
			{Term: args.LastIncludedTerm}, // 哨兵条目
		}
	} else {
		// 保留快照之后的日志条目
		relativeIndex := args.LastIncludedIndex - rf.lastIncludedIndex

		if relativeIndex > 0 && relativeIndex < len(rf.log) {
			// 验证索引处的任期是否匹配
			if rf.log[relativeIndex].Term == args.LastIncludedTerm {
				// 保留快照后的日志条目
				newLog = make([]LogEntry, 1)
				newLog[0] = LogEntry{Term: args.LastIncludedTerm} // 哨兵条目
				newLog = append(newLog, rf.log[relativeIndex+1:]...)
			} else {
				// 任期不匹配，丢弃所有日志
				newLog = []LogEntry{
					{Term: args.LastIncludedTerm}, // 哨兵条目
				}
			}
		} else {
			// 索引越界，丢弃所有日志
			newLog = []LogEntry{
				{Term: args.LastIncludedTerm}, // 哨兵条目
			}
		}
	}

	// 更新状态
	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// 更新commitIndex和lastApplied（不能回退）
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	oldLastApplied := rf.lastApplied

	// 确保lastApplied不回退
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	// 保存快照和状态
	rf.persistStateAndSnapshot(args.Data)

	// 准备快照消息
	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// 如果需要应用快照（lastApplied之前小于快照索引）
	applySnapshot := oldLastApplied < args.LastIncludedIndex

	// 保存通道引用
	applyCh := rf.applyCh

	rf.mu.Unlock()

	// 在锁外发送快照消息
	if applySnapshot {
		applyCh <- snapshotMsg
	}
}

// 合并sendHeartbeats和replicateLog
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}

			args.PrevLogIndex = rf.nextIndex[server] - 1
			nextIndex := rf.nextIndex[server]

			if args.PrevLogIndex < rf.lastIncludedIndex {
				rf.mu.Unlock()

				rf.mu.Lock()
				snapshot := rf.persister.ReadSnapshot()
				arg := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              snapshot,
				}
				currentTerm := rf.currentTerm
				rf.mu.Unlock()

				reply := &InstallSnapshotReply{}

				if ok := rf.sendInstallSnapshot(server, arg, reply); !ok {
					return
				}

				rf.mu.Lock()
				if rf.currentTerm != currentTerm || rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
					return
				}

				rf.nextIndex[server] = arg.LastIncludedIndex + 1
				rf.matchIndex[server] = arg.LastIncludedIndex

				rf.checkCommit()
				rf.mu.Unlock()
			} else {
				lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1

				if nextIndex <= lastLogIndex {
					relativeNextIndex := nextIndex - rf.lastIncludedIndex
					if relativeNextIndex > 0 && relativeNextIndex < len(rf.log) {
						args.Entries = make([]LogEntry, len(rf.log)-relativeNextIndex)
						copy(args.Entries, rf.log[relativeNextIndex:])
					}
				}

				relativeIndex := args.PrevLogIndex - rf.lastIncludedIndex
				if args.PrevLogIndex == rf.lastIncludedIndex {
					args.PrevLogTerm = rf.lastIncludedTerm
				} else if relativeIndex > 0 && relativeIndex < len(rf.log) {
					args.PrevLogTerm = rf.log[relativeIndex].Term
				} else {
					rf.mu.Unlock()
					return
				}

				currentTerm := rf.currentTerm
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				if rf.currentTerm != currentTerm || rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					newMatchIndex := args.PrevLogIndex + len(args.Entries)
					if newMatchIndex > rf.matchIndex[server] {
						rf.matchIndex[server] = newMatchIndex
					}
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					rf.checkCommit()
					rf.mu.Unlock()
				} else {
					if reply.ConflictTerm != -1 {
						conflictTermLastIndex := -1
						for i := len(rf.log) - 1; i >= 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								conflictTermLastIndex = i + rf.lastIncludedIndex
								break
							}
						}

						if conflictTermLastIndex != -1 {
							rf.nextIndex[server] = conflictTermLastIndex + 1
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
					} else if reply.ConflictIndex != -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						if rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
						}
					}

					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}

					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

// RequestLogReplay 请求重放指定范围内的日志条目
// startIndex: 开始索引（包含）
// endIndex: 结束索引（包含）
// 如果endIndex为-1，则重放到最新的已提交日志
func (rf *Raft) RequestLogReplay(startIndex int, endIndex int) {
	rf.mu.Lock()

	validStartIndex := max(startIndex, rf.lastIncludedIndex+1)

	validEndIndex := rf.commitIndex
	if endIndex != -1 {
		validEndIndex = min(endIndex, rf.commitIndex)
	}

	if validStartIndex > validEndIndex {
		rf.mu.Unlock()
		return
	}

	entriesToReplay := make([]ApplyMsg, 0)
	for i := validStartIndex; i <= validEndIndex; i++ {
		logIndex := i - rf.lastIncludedIndex

		if logIndex <= 0 || logIndex >= len(rf.log) {
			continue
		}

		entry := rf.log[logIndex]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: i,
		}
		entriesToReplay = append(entriesToReplay, msg)
	}

	rf.mu.Unlock()

	for _, msg := range entriesToReplay {
		rf.applyCh <- msg
	}
}
