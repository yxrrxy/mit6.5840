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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	}

	// 检查是否可以投票（未投票或已投票给相同候选人）
	// 同时检查候选人的日志是否至少与自己一样新
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	// 日志比较规则：
	// 1. 如果任期不同，更高任期的日志更新
	// 2. 如果任期相同，更长的日志更新
	logIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logIsUpToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logIsUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now() // 重置选举超时
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

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// 添加新的日志条目
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	index := len(rf.log) - 1 // 新日志的索引

	// 立即尝试复制日志
	go rf.replicateLog()

	return index, rf.currentTerm, true
}

func (rf *Raft) replicateLog() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	// 确保nextIndex和matchIndex已初始化
	if len(rf.nextIndex) != len(rf.peers) {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.log) //后面会实现回退
			rf.matchIndex[i] = 0
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.sendLogTo(server)
			}(i)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendLogTo(server int) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		// 如果不再是Leader，退出
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		if len(rf.nextIndex) <= server || len(rf.log) == 0 {
			rf.mu.Unlock()
			continue
		}

		// 检查是否有新日志需要发送
		nextIdx := rf.nextIndex[server]
		if nextIdx >= len(rf.log) && rf.commitIndex <= rf.matchIndex[server] {
			// 没有新日志需要发送，也没有需要更新的commitIndex
			rf.mu.Unlock()
			continue
		}

		if nextIdx >= len(rf.log) {
			nextIdx = len(rf.log)
			rf.nextIndex[server] = nextIdx
		}

		prevLogIndex := nextIdx - 1

		// 确保prevLogIndex是有效的
		if prevLogIndex < 0 {
			prevLogIndex = 0
		}

		var prevLogTerm int
		if prevLogIndex < len(rf.log) {
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		// 准备要发送的条目
		var entries []LogEntry
		if nextIdx < len(rf.log) {
			entries = make([]LogEntry, len(rf.log)-nextIdx)
			copy(entries, rf.log[nextIdx:])
		} else {
			entries = []LogEntry{}
		}

		// 准备AppendEntries参数
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		// 发送AppendEntries RPC
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)

		if ok {
			rf.mu.Lock()

			// 如果任期变化或不再是Leader，停止处理
			if rf.currentTerm != currentTerm || rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			// 处理更高任期
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}

			// 处理成功响应
			if reply.Success {
				// 更新匹配索引
				newMatchIndex := prevLogIndex + len(entries)
				if newMatchIndex > rf.matchIndex[server] {
					rf.matchIndex[server] = newMatchIndex
				}
				rf.nextIndex[server] = newMatchIndex + 1

				// 检查是否可以提交新的日志
				rf.checkCommit()
				rf.mu.Unlock()
				return
			} else {
				// 处理日志不匹配
				if reply.ConflictTerm != -1 {
					// 查找Leader日志中最后一个与冲突任期匹配的条目
					conflictTermLastIndex := -1
					for i := len(rf.log) - 1; i >= 1; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							conflictTermLastIndex = i
							break
						}
					}

					if conflictTermLastIndex != -1 {
						// 如果找到了匹配的任期，跳到该任期的下一个位置
						rf.nextIndex[server] = conflictTermLastIndex + 1
					} else {
						// 如果没找到匹配的任期，跳到Follower冲突任期的第一个日志
						rf.nextIndex[server] = reply.ConflictIndex
					}
				} else if reply.ConflictIndex >= 0 {
					// Follower的日志比Leader短，直接跳到Follower的日志末尾
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					// 保守策略：如果没有明确的冲突信息，就回退一步
					if rf.nextIndex[server] > 1 {
						rf.nextIndex[server]--
					}
				}

				// 确保nextIndex至少为1
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}

				// 立即尝试再次发送 - 加快同步速度
				//go rf.sendLogTo(server)

				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) checkCommit() {
	for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
		// 只考虑提交当前任期的日志条目（安全性保证）
		if rf.log[i].Term == rf.currentTerm {
			count := 1 // 包括自己
			for server := range rf.peers {
				if server != rf.me && rf.matchIndex[server] >= i {
					count++
				}
			}
			// 如果大多数服务器已复制此日志条目，提交它
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				// 启动日志应用
				if !rf.applyLogsActive {
					rf.applyLogsActive = true
					go rf.applyLogs()
				}
				break
			}
		}
	}
}

func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			toApply := make([]ApplyMsg, 0)
			lastApplied := rf.lastApplied // 保存当前值

			// 准备要应用的日志
			for i := lastApplied + 1; i <= rf.commitIndex; i++ {
				if i < len(rf.log) && rf.log[i].Command != nil { // 确保命令非空
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
					toApply = append(toApply, msg)
				}
			}

			rf.mu.Unlock()

			// 应用日志
			for _, msg := range toApply {
				rf.applyCh <- msg

				// 应用后更新lastApplied
				rf.mu.Lock()
				if msg.CommandIndex > rf.lastApplied {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond) // 减少等待时间，更快响应
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
		rf.mu.Lock()
		if time.Since(rf.lastHeartbeat) > getRandomTimeout() {
			rf.startElection()
		}
		rf.mu.Unlock()
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 60 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	// 获取自己最后一条日志的索引和任期
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
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
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for !rf.killed() {
		if rf.state != Leader {
			return
		}

		// 向所有节点发送心跳
		for i := range rf.peers {
			if i != rf.me {
				go func(server int) {
					rf.mu.Lock()

					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}

					// 计算prevLogIndex和prevLogTerm
					prevLogIndex := rf.nextIndex[server] - 1
					var prevLogTerm int
					if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
						prevLogTerm = rf.log[prevLogIndex].Term
					}

					// 准备心跳请求（可能包含日志条目）
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						LeaderCommit: rf.commitIndex,
						// 心跳一般不包含条目，但如果有未复制的条目，也可以一并发送
						Entries: make([]LogEntry, 0),
					}

					rf.mu.Unlock()

					// 发送请求
					reply := AppendEntriesReply{}
					if rf.sendAppendEntries(server, &args, &reply) {
						rf.mu.Lock()

						// 如果收到更高任期，转为Follower
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.mu.Unlock()
							return
						}

						rf.mu.Unlock()
					}
				}(i)
			}
		}

		time.Sleep(300 * time.Millisecond)
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
	}

	// 更新心跳时间，确保在任何情况下都重置心跳时间
	rf.lastHeartbeat = time.Now()
	rf.state = Follower // 确保节点是Follower状态

	// 确保日志数组已初始化
	if len(rf.log) == 0 {
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{Term: 0}
	}

	// 日志一致性检查
	if args.PrevLogIndex >= len(rf.log) {
		// 日志太短
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 任期不匹配
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		// 找到该冲突任期的第一个日志条目
		reply.ConflictIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= 1; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		return
	}
	// 附加新日志
	reply.Success = true

	// 处理日志条目 - 修改这部分代码确保正确处理大命令
	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			idx := args.PrevLogIndex + 1 + i
			if idx >= len(rf.log) {
				// 追加新条目
				rf.log = append(rf.log, entry)
			} else if rf.log[idx].Term != entry.Term {
				// 删除不匹配的条目及其后的所有条目
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
			}
			// 如果任期匹配，则保留原有条目
		}
	}

	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, len(rf.log)-1)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			go rf.applyLogs()
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
		peers:           peers,
		persister:       persister,
		me:              me,
		applyCh:         applyCh,
		state:           Follower,
		currentTerm:     0,
		votedFor:        -1,
		lastHeartbeat:   time.Now(),
		log:             make([]LogEntry, 1), // 从索引1开始，0是哨兵
		commitIndex:     0,                   // 初始时没有提交任何日志
		lastApplied:     0,                   // 初始时没有应用任何日志
		nextIndex:       nil,                 // 只有Leader才需要初始化这些
		matchIndex:      nil,                 // 只有Leader才需要初始化这些
		applyLogsActive: false,               // 初始时未启动日志应用协程
	}

	// 索引0是不使用的哨兵条目，Term为0
	rf.log[0] = LogEntry{Term: 0}

	// 从持久化存储中恢复状态（如果有）
	rf.readPersist(persister.ReadRaftState())

	// 启动周期性检查选举超时的goroutine
	go rf.ticker()

	return rf
}

// 成为Leader时初始化状态
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.applyLogsActive = false // 确保当成为新Leader时重置此标志

	// 初始化nextIndex和matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) // 初始化为自己的日志长度
		rf.matchIndex[i] = 0          // 初始时假设没有匹配
	}

	// 记录成为Leader的时间
	rf.lastHeartbeat = time.Now()

	// 立即发送心跳以建立权威
	go rf.broadcastHeartbeat()
}
