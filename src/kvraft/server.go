package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

type Op struct {
	SeqId    int64
	Key      string
	Value    string
	ClientId int64
	Index    int
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	seqMap    map[int64]int64   //为了确保seq只执行一次	clientId / seqId
	waitChMap map[int]chan Op   //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
	kvPersist map[string]string // 存储持久化的KV键值对	K / V

	lastIncludeIndex int             // 最近一次快照的截止的日志索引
	lastApplied      int             // 最后应用的命令索引
	persister        *raft.Persister // 共享raft的持久化地址，方便查找

	replayComplete bool           // 标记初始日志重放是否完成
	initialClients map[int64]bool // 记录启动时正在重放日志的客户端ID
	replayDone     chan struct{}  // 信号通道，日志重放完成后发送信号
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if lastSeqId, exists := kv.seqMap[args.ClientID]; exists && args.RequestID <= lastSeqId {
		reply.Value = kv.kvPersist[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:   GetOp,
		Key:      args.Key,
		SeqId:    args.RequestID,
		ClientId: args.ClientID,
	}

	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(300 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if lastSeqId, exists := kv.seqMap[args.ClientID]; exists && args.RequestID <= lastSeqId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	replayComplete := kv.replayComplete
	isRecoveryRequest := false

	if !replayComplete {
		isRecoveryRequest = int64(args.RequestID) <= int64(kv.lastApplied) ||
			kv.initialClients[args.ClientID] ||
			args.Op == AppendOp

		if !isRecoveryRequest {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		SeqId:    args.RequestID,
		ClientId: args.ClientID,
	}

	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(3000 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *KVServer) applyMsgHandlerLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			kv.mu.Lock()

			if !kv.replayComplete {
			} else if index <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			lastSeqId, exist := kv.seqMap[op.ClientId]
			isDup := exist && op.SeqId <= lastSeqId

			if !isDup {
				switch op.OpType {
				case PutOp:
					kv.kvPersist[op.Key] = op.Value
				case AppendOp:
					currentValue, exists := kv.kvPersist[op.Key]
					if !exists {
						currentValue = ""
					}
					kv.kvPersist[op.Key] = currentValue + op.Value
				case GetOp:
				}
				kv.seqMap[op.ClientId] = op.SeqId
			}

			if index > kv.lastApplied {
				kv.lastApplied = index
			}

			if !kv.replayComplete && (index > 10 || op.OpType == AppendOp) {
				kv.replayComplete = true
				close(kv.replayDone)
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				ch, exist := kv.waitChMap[index]
				if exist {
					select {
					case ch <- op:
					default:
					}
				}
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				if e.Encode(kv.kvPersist) != nil ||
					e.Encode(kv.seqMap) != nil ||
					e.Encode(kv.lastApplied) != nil {
				} else {
					snapshot := w.Bytes()
					kv.rf.Snapshot(kv.lastApplied, snapshot)
					kv.lastIncludeIndex = kv.lastApplied
				}
			}

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.lastApplied < msg.SnapshotIndex {
				snapshot := msg.Snapshot
				if snapshot != nil && len(snapshot) > 0 {
					r := bytes.NewBuffer(snapshot)
					d := labgob.NewDecoder(r)

					var kvPersist map[string]string
					var seqMap map[int64]int64
					var lastIncludeIndex int

					decodeErr := false
					if d.Decode(&kvPersist) != nil ||
						d.Decode(&seqMap) != nil ||
						d.Decode(&lastIncludeIndex) != nil {
						decodeErr = true
					}

					if !decodeErr {
						kv.kvPersist = make(map[string]string)
						for k, v := range kvPersist {
							kv.kvPersist[k] = v
						}
						kv.seqMap = make(map[int64]int64)
						for k, v := range seqMap {
							kv.seqMap[k] = v
						}
						kv.lastIncludeIndex = lastIncludeIndex
						kv.lastApplied = lastIncludeIndex

						kv.replayComplete = false
						if kv.replayDone == nil || isClosed(kv.replayDone) {
							kv.replayDone = make(chan struct{})
						}

						for idx := range kv.waitChMap {
							if idx <= lastIncludeIndex {
								close(kv.waitChMap[idx])
								delete(kv.waitChMap, idx)
							}
						}
					}
				}
			}
			kv.mu.Unlock()
		}
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.seqMap = make(map[int64]int64)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)
	kv.lastIncludeIndex = 0
	kv.lastApplied = 0

	kv.replayComplete = false
	kv.initialClients = make(map[int64]bool)
	kv.replayDone = make(chan struct{})

	snapshot := persister.ReadSnapshot()
	raftStateSize := persister.RaftStateSize()

	needLogReplay := false
	startReplayIndex := 0

	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)

		var kvPersist map[string]string
		var seqMap map[int64]int64
		var lastIncludeIndex int

		decodeErr := false
		if d.Decode(&kvPersist) != nil ||
			d.Decode(&seqMap) != nil ||
			d.Decode(&lastIncludeIndex) != nil {
			decodeErr = true
		}

		if !decodeErr {
			if kvPersist != nil {
				for k, v := range kvPersist {
					kv.kvPersist[k] = v
				}
			}
			if seqMap != nil {
				for k, v := range seqMap {
					kv.seqMap[k] = v
				}
			}
			kv.lastIncludeIndex = lastIncludeIndex
			kv.lastApplied = lastIncludeIndex

			needLogReplay = true
			startReplayIndex = lastIncludeIndex + 1

			kv.replayComplete = false
		}
	} else if raftStateSize > 0 {
		needLogReplay = true
		startReplayIndex = 1
		kv.replayComplete = false
	} else {
		kv.replayComplete = true
		close(kv.replayDone)
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgHandlerLoop()

	if needLogReplay {
		go func() {
			time.Sleep(100 * time.Millisecond)
			kv.rf.RequestLogReplay(startReplayIndex, -1)
		}()
	}

	return kv
}
