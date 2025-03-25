package shardctrler

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type OpType string

const (
	JoinOp  OpType = "Join"
	LeaveOp OpType = "Leave"
	MoveOp  OpType = "Move"
	QueryOp OpType = "Query"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied int
	clientSeq   map[int64]int
	configs     []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type     OpType
	ClientId int64
	SeqNum   int
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	index, _, isLeader := sc.rf.Start(Op{
		Type:     JoinOp,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Servers:  args.Servers,
	})

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	for i := 0; i < 10; i++ {
		sc.mu.Lock()
		if sc.lastApplied >= index {
			// 检查是否成功处理了请求
			if lastSeq, ok := sc.clientSeq[args.ClientId]; ok && lastSeq >= args.SeqNum {
				reply.Err = OK
			} else {
				reply.Err = "timeout"
			}
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	reply.Err = "timeout"
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	index, _, isLeader := sc.rf.Start(Op{
		Type:     LeaveOp,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		GIDs:     args.GIDs,
	})

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 等待操作应用
	for i := 0; i < 10; i++ {
		sc.mu.Lock()
		if sc.lastApplied >= index {
			// 检查是否成功处理了请求
			if lastSeq, ok := sc.clientSeq[args.ClientId]; ok && lastSeq >= args.SeqNum {
				reply.Err = OK
			} else {
				reply.Err = "timeout"
			}
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	reply.Err = "timeout"
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	index, _, isLeader := sc.rf.Start(Op{
		Type:     MoveOp,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Shard:    args.Shard,
		GID:      args.GID,
	})

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	for i := 0; i < 10; i++ {
		sc.mu.Lock()
		if sc.lastApplied >= index {
			// 检查是否成功处理了请求
			if lastSeq, ok := sc.clientSeq[args.ClientId]; ok && lastSeq >= args.SeqNum {
				reply.Err = OK
			} else {
				reply.Err = "timeout"
			}
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	reply.Err = "timeout"
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	index, _, isLeader := sc.rf.Start(Op{
		Type:     QueryOp,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Num:      args.Num,
	})

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	for i := 0; i < 10; i++ {
		sc.mu.Lock()
		if sc.lastApplied >= index {
			// Query不需要去重检查，直接返回结果
			if args.Num == -1 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
				reply.Config.Num = args.Num
			}
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	reply.Err = "timeout"
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyLoop() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastApplied {
				sc.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)
			sc.lastApplied = msg.CommandIndex

			if lastSeq, ok := sc.clientSeq[op.ClientId]; !ok || lastSeq < op.SeqNum {
				sc.clientSeq[op.ClientId] = op.SeqNum

				// 只有非Query操作需要修改配置
				if op.Type != QueryOp {
					switch op.Type {
					case JoinOp:
						sc.applyJoin(op.Servers)
					case LeaveOp:
						sc.applyLeave(op.GIDs)
					case MoveOp:
						sc.applyMove(op.Shard, op.GID)
					}
				}
			}

			sc.mu.Unlock()
		} else if msg.SnapshotValid {
			// 处理快照
			sc.mu.Lock()
			sc.readSnapshot(msg.Snapshot)
			sc.lastApplied = msg.SnapshotIndex
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) applyJoin(servers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfigNum := len(sc.configs)

	newConfig := Config{
		Num:    newConfigNum,
		Groups: make(map[int][]string),
		Shards: lastConfig.Shards,
	}

	for gid, servs := range lastConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servs...)
	}

	for gid, servs := range servers {
		newServers := make([]string, len(servs))
		copy(newServers, servs)
		newConfig.Groups[gid] = newServers
	}

	sc.rebalanceShards(&newConfig)
	sc.configs = append(sc.configs, newConfig)

	sc.createSnapshot()
}

func (sc *ShardCtrler) applyLeave(gids []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfigNum := len(sc.configs)

	newConfig := Config{
		Num:    newConfigNum,
		Groups: make(map[int][]string),
		Shards: lastConfig.Shards,
	}

	for gid, servs := range lastConfig.Groups {
		shouldRemove := false
		for _, g := range gids {
			if gid == g {
				shouldRemove = true
				break
			}
		}
		if !shouldRemove {
			newConfig.Groups[gid] = append([]string{}, servs...)
		}
	}

	// 如果所有组都被删除，将所有分片分配给GID 0
	if len(newConfig.Groups) == 0 {
		for i := range newConfig.Shards {
			newConfig.Shards[i] = 0
		}
	} else {
		// 重新平衡分片
		sc.rebalanceShards(&newConfig)
	}

	sc.configs = append(sc.configs, newConfig)

	sc.createSnapshot()
}

func (sc *ShardCtrler) applyMove(shard int, gid int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfigNum := len(sc.configs)

	newConfig := Config{
		Num:    newConfigNum,
		Groups: make(map[int][]string),
		Shards: lastConfig.Shards,
	}

	for gid, servs := range lastConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servs...)
	}

	newConfig.Shards[shard] = gid

	sc.configs = append(sc.configs, newConfig)

	sc.createSnapshot()
}

func (sc *ShardCtrler) createSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.clientSeq)
	snapshot := w.Bytes()

	sc.rf.Snapshot(sc.lastApplied, snapshot)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	// 所有分片分配给GID 0
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(map[int][]string{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientSeq = make(map[int64]int)
	sc.lastApplied = 0

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sc.readSnapshot(snapshot)
	}

	go sc.applyLoop()

	return sc
}

func (sc *ShardCtrler) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var configs []Config
	var clientSeq map[int64]int

	if d.Decode(&configs) != nil || d.Decode(&clientSeq) != nil {
	} else {
		sc.configs = configs
		sc.clientSeq = clientSeq
	}
}

func (sc *ShardCtrler) rebalanceShards(config *Config) {
	//没有组，给GID 0
	if len(config.Groups) == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	var gids []int
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	//map 的迭代顺序是随机的
	sort.Ints(gids)

	shardsPerGroup := NShards / len(gids)
	extraShards := NShards % len(gids)

	gidToShards := make(map[int][]int)
	for i, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], i)
	}

	var allOrphanShards []int
	if shards, ok := gidToShards[0]; ok {
		allOrphanShards = append(allOrphanShards, shards...)
		delete(gidToShards, 0)
	}

	for gid, shards := range gidToShards {
		if _, exists := config.Groups[gid]; !exists {
			allOrphanShards = append(allOrphanShards, shards...)
			delete(gidToShards, gid)
		}
	}

	sort.Ints(allOrphanShards)

	targetCount := make(map[int]int)
	for i, gid := range gids {
		targetCount[gid] = shardsPerGroup
		if i < extraShards {
			targetCount[gid]++
		}
	}

	//多余的分片放入orphanShards
	for _, gid := range gids {
		shards := gidToShards[gid]
		sort.Ints(shards)
		for len(shards) > targetCount[gid] {
			allOrphanShards = append(allOrphanShards, shards[len(shards)-1])
			shards = shards[:len(shards)-1]
		}
		gidToShards[gid] = shards
	}

	sort.Ints(allOrphanShards)

	for _, gid := range gids {
		for len(gidToShards[gid]) < targetCount[gid] && len(allOrphanShards) > 0 {
			gidToShards[gid] = append(gidToShards[gid], allOrphanShards[0])
			allOrphanShards = allOrphanShards[1:]
		}
	}

	//将每个分片分配给组
	var newAssignment [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newAssignment[shard] = gid
		}
	}

	config.Shards = newAssignment
}
