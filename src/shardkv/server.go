package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	OpGet           = "Get"
	OpPut           = "Put"
	OpAppend        = "Append"
	OpConfig        = "Config"        // 配置更新
	OpTransfer      = "Transfer"      // 接收分片
	OpCleanup       = "Cleanup"       // 清理分片
	OpInstallShards = "InstallShards" // 安装多个分片

	// 分片状态
	ShardNormal    = 0 // 正常状态
	ShardMigrating = 1 // 迁移中状态
	ShardWaiting   = 2 // 等待接收状态
)

// Op represents an operation to be applied to the state machine
type Op struct {
	Type     string // "Get", "Put", or "Append"
	Key      string
	Value    string
	ClientId int64
	SeqNum   int

	Config    shardctrler.Config
	Shard     int
	ConfigNum int
	ShardData map[int]map[string]string
	ClientSeq map[int64]int
	Shards    []int
}

// OpResult 表示操作的结果
type OpResult struct {
	ClientId int64
	SeqNum   int
	Value    string
	Err      Err
}

// ShardKV represents a shard key-value server
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32
	mck         *shardctrler.Clerk
	config      shardctrler.Config
	prevConfig  shardctrler.Config
	persister   *raft.Persister
	lastApplied int

	lastIncludeIndex int // 最近一次快照的截止的日志索引

	clientSeq     map[int64]int
	data          map[int]map[string]string         // 按分片存储数据 map[shard][key]value
	waitingShards map[int]bool                      // 等待从其他组接收的分片 map[shard]bool
	outShards     map[int]map[int]map[string]string // 需要发送给其他组的分片 map[configNum][shard][key]value

	// 用于同步 RPC 调用的通知
	notifyCh map[int]chan OpResult

	// 日志重放
	ReplayComplete bool           // 标记初始日志重放是否完成
	InitialClients map[int64]bool // 记录启动时正在重放日志的客户端ID
	ReplayDone     chan struct{}  // 信号通道，日志重放完成后发送信号

	// 分片状态映射
	shardStates map[int]int // 记录每个分片的状态

	// 最后一次配置变更时间
	LastConfigTime time.Time
}

// 后面可能还要改，所以留着日志输出
func DPrintf(format string, a ...interface{}) {
	if false {
		log.Printf(format, a...)
	}
}

// Get method receives a Get RPC request from client
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)

	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// 检查该分片是否属于本组
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 检查分片状态
	if kv.shardStates[shard] == ShardMigrating || kv.shardStates[shard] == ShardWaiting {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 检查该分片是否正在等待迁移
	if _, waiting := kv.waitingShards[shard]; waiting {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 检查分片数据是否存在
	if _, ok := kv.data[shard]; !ok {
		kv.data[shard] = make(map[string]string)
	}

	// 在重放完成前不接受新请求，除非是恢复请求
	if !kv.ReplayComplete {
		isRecoveryRequest := kv.InitialClients[args.ClientId]
		if !isRecoveryRequest {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
	}

	// 检查是否是重复请求，如果是，直接返回上次的结果
	if lastSeq, exists := kv.clientSeq[args.ClientId]; exists && args.SeqNum <= lastSeq {
		// 对于Get请求，如果是重复的，可以直接返回值
		if value, exists := kv.data[shard][args.Key]; exists {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	// 创建操作对象并提交到 Raft
	op := Op{
		Type:     OpGet,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	// 提交操作到 Raft 并等待结果
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		if result.ClientId == args.ClientId && result.SeqNum == args.SeqNum {
			reply.Err = result.Err
			reply.Value = result.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()
}

// PutAppend method receives a PutAppend RPC request from client
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard := key2shard(args.Key)

	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// 检查该分片是否属于本组
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 检查分片状态
	if kv.shardStates[shard] == ShardMigrating || kv.shardStates[shard] == ShardWaiting {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 检查该分片是否正在等待迁移
	if _, waiting := kv.waitingShards[shard]; waiting {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	replayComplete := kv.ReplayComplete
	isRecoveryRequest := kv.InitialClients[args.ClientId]

	if !replayComplete && !isRecoveryRequest {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// 检查是否是重复请求，如果是，直接返回成功
	if lastSeq, exists := kv.clientSeq[args.ClientId]; exists && args.SeqNum <= lastSeq {
		// 重复请求直接返回成功
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	// 创建操作对象并提交到 Raft
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		if result.ClientId == args.ClientId && result.SeqNum == args.SeqNum {
			reply.Err = result.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()
}

// 根据配置历史确定分片的可能所有者
func (kv *ShardKV) findPotentialOwners(shard int, configNum int) map[int][]string {
	potentialOwners := make(map[int][]string)

	if configNum <= kv.config.Num {
		if kv.prevConfig.Num > 0 {
			prevOwner := kv.prevConfig.Shards[shard]
			if prevOwner != 0 {
				if servers, ok := kv.prevConfig.Groups[prevOwner]; ok {
					potentialOwners[prevOwner] = servers
					DPrintf("Group %d Server %d findPotentialOwners: 为分片 %d 找到前一配置 %d 中的所有者 %d",
						kv.gid, kv.me, shard, kv.prevConfig.Num, prevOwner)
				}
			}
		}
	}

	// 维护一个已检查的配置集合
	checkedConfigs := make(map[int]bool)
	if kv.prevConfig.Num > 0 {
		checkedConfigs[kv.prevConfig.Num] = true
	}

	const maxLookback = 3
	startConfig := configNum

	// 从当前配置向前查找几个配置
	for i := 0; i < maxLookback && startConfig > 0; i++ {
		if _, checked := checkedConfigs[startConfig]; !checked {
			config := kv.mck.Query(startConfig)
			if config.Num == startConfig {
				checkedConfigs[startConfig] = true
				owner := config.Shards[shard]
				if owner != 0 && owner != kv.gid {
					if servers, ok := config.Groups[owner]; ok {
						if _, exists := potentialOwners[owner]; !exists {
							potentialOwners[owner] = servers
							DPrintf("Group %d Server %d findPotentialOwners: 为分片 %d 找到配置 %d 中的所有者 %d",
								kv.gid, kv.me, shard, startConfig, owner)
						}
					}
				}
			}
		}
		startConfig--
	}

	return potentialOwners
}

// 定期尝试从其他组拉取分片
func (kv *ShardKV) pullShardsLoop() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if !kv.ReplayComplete {
			select {
			case <-kv.ReplayDone:
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		kv.mu.Lock()
		if len(kv.waitingShards) == 0 {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		currConfig := kv.config
		waitingShards := make([]int, 0)
		for shard := range kv.waitingShards {
			waitingShards = append(waitingShards, shard)
		}

		// 获取所有分片的所有者
		potentialOwnersMap := make(map[int]map[int][]string)
		for _, shard := range waitingShards {
			potentialOwnersMap[shard] = kv.findPotentialOwners(shard, currConfig.Num)
		}
		kv.mu.Unlock()

		// 记录成功获取的分片数据
		successfulShards := make(map[int]map[string]string)
		successfulClientSeqs := make(map[int]map[int64]int)

		// 记录每个分片的重试次数
		retryCount := make(map[int]int)
		maxRetries := 1

		// 串行处理每个分片
		for _, shard := range waitingShards {
			// 检查重试次数
			if retryCount[shard] >= maxRetries {
				DPrintf("Group %d Server %d pullShardsLoop: 分片 %d 达到最大重试次数 %d, 跳过",
					kv.gid, kv.me, shard, maxRetries)
				continue
			}

			// 获取该分片的所有可能所有者
			owners := potentialOwnersMap[shard]
			if len(owners) == 0 {
				DPrintf("Group %d Server %d pullShardsLoop: 分片 %d 没有找到潜在所有者",
					kv.gid, kv.me, shard)
				continue
			}

			// 尝试从每个可能所有者获取分片
			shardAcquired := false
			for gid, servers := range owners {
				if shardAcquired {
					break
				}
				for si := 0; si < len(servers); si++ {
					args := GetShardArgs{
						Shard:     shard,
						ConfigNum: currConfig.Num,
					}
					reply := GetShardReply{}

					serverEnd := kv.make_end(servers[si])
					ok := serverEnd.Call("ShardKV.GetShard", &args, &reply)

					if !ok {
						DPrintf("Group %d Server %d pullShardsLoop: 从组 %d 服务器 %s 获取分片 %d 失败: RPC错误",
							kv.gid, kv.me, gid, servers[si], shard)
						continue
					}

					if reply.Err == OK {
						if _, exists := successfulShards[shard]; !exists {
							successfulShards[shard] = make(map[string]string)
						}
						for k, v := range reply.ShardData {
							successfulShards[shard][k] = v
						}
						if _, exists := successfulClientSeqs[shard]; !exists {
							successfulClientSeqs[shard] = make(map[int64]int)
						}
						for clientId, seqNum := range reply.ClientSeq {
							successfulClientSeqs[shard][clientId] = seqNum
						}
						shardAcquired = true
						break
					}
				}
			}

			if !shardAcquired {
				retryCount[shard]++
				DPrintf("Group %d Server %d pullShardsLoop: 分片 %d 获取失败, 重试次数 %d/%d",
					kv.gid, kv.me, shard, retryCount[shard], maxRetries)
			}
		}

		if len(successfulShards) > 0 {
			kv.mu.Lock()
			shardsToInstall := make([]int, 0)
			for shard := range successfulShards {
				if _, waiting := kv.waitingShards[shard]; waiting {
					shardsToInstall = append(shardsToInstall, shard)
				}
			}

			if len(shardsToInstall) > 0 {
				op := Op{
					Type:      OpInstallShards,
					Shards:    shardsToInstall,
					ShardData: successfulShards,
					ClientSeq: make(map[int64]int),
					ConfigNum: currConfig.Num,
				}

				for _, clientSeq := range successfulClientSeqs {
					for clientId, seqNum := range clientSeq {
						if lastSeq, ok := op.ClientSeq[clientId]; !ok || seqNum > lastSeq {
							op.ClientSeq[clientId] = seqNum
						}
					}
				}

				kv.mu.Unlock()
				kv.rf.Start(op)
			} else {
				kv.mu.Unlock()
			}
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// 辅助函数：计算所有分片中的键总数
func countTotalKeys(shards map[int]map[string]string) int {
	count := 0
	for _, data := range shards {
		count += len(data)
	}
	return count
}

// 处理其他组的分片获取请求
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	// 初始化回复结构
	reply.ShardData = make(map[string]string)
	reply.ClientSeq = make(map[int64]int)
	reply.Err = ErrWrongGroup

	// 检查是否是领导者
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Group %d Server %d GetShard: 非领导者收到分片 %d 配置 %d 的请求",
			kv.gid, kv.me, args.Shard, args.ConfigNum)
		return
	}

	// 等待日志重放完成
	if !kv.ReplayComplete {
		select {
		case <-kv.ReplayDone:
		case <-time.After(100 * time.Millisecond):
			reply.Err = ErrWrongLeader
			return
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Group %d Server %d GetShard: 收到请求获取分片 %d 配置 %d",
		kv.gid, kv.me, args.Shard, args.ConfigNum)

	if args.ConfigNum < kv.config.Num {
		DPrintf("Group %d Server %d GetShard: 请求的配置号 %d 小于当前配置号 %d，尝试提供历史数据",
			kv.gid, kv.me, args.ConfigNum, kv.config.Num)

		if outShards, exists := kv.outShards[args.ConfigNum]; exists {
			if shardData, found := outShards[args.Shard]; found {
				for k, v := range shardData {
					reply.ShardData[k] = string([]byte(v))
				}

				for clientId, seqNum := range kv.clientSeq {
					reply.ClientSeq[clientId] = seqNum
				}

				DPrintf("Group %d Server %d GetShard: 为旧配置 %d 提供分片 %d 的历史数据",
					kv.gid, kv.me, args.ConfigNum, args.Shard)
				reply.Err = OK
				return
			}
		}
	}

	// 首先尝试从outShards中寻找数据
	if outShards, exists := kv.outShards[args.ConfigNum]; exists {
		if shardData, found := outShards[args.Shard]; found {
			// 找到了备份的分片数据
			keyCount := len(shardData)
			dataSize := 0

			for k, v := range shardData {
				reply.ShardData[k] = string([]byte(v))
				dataSize += len(v)
			}

			clientCount := 0
			for clientId, seqNum := range kv.clientSeq {
				reply.ClientSeq[clientId] = seqNum
				clientCount++
			}

			DPrintf("Group %d Server %d GetShard: 完成分片 %d 备份数据发送，总键值对: %d，数据大小: %d字节，客户端序列号: %d",
				kv.gid, kv.me, args.Shard, keyCount, dataSize, clientCount)
			reply.Err = OK
			return
		}
	}

	// 如果在当前配置的outShards中找不到,尝试在前一个配置中查找
	if outShards, exists := kv.outShards[args.ConfigNum-1]; exists {
		if shardData, found := outShards[args.Shard]; found {
			// 找到了前一个配置的备份数据
			for k, v := range shardData {
				reply.ShardData[k] = string([]byte(v))
			}
			for clientId, seqNum := range kv.clientSeq {
				reply.ClientSeq[clientId] = seqNum
			}
			DPrintf("Group %d Server %d GetShard: 在前一个配置 %d 中找到分片 %d 的数据",
				kv.gid, kv.me, args.ConfigNum-1, args.Shard)
			reply.Err = OK
			return
		}
	}

	// 如果是请求旧版本的分片，查找我们当前持有的分片数据
	if args.ConfigNum < kv.config.Num {
		if _, exists := kv.data[args.Shard]; exists {
			// 创建当前分片数据的副本
			keyCount := 0
			for k, v := range kv.data[args.Shard] {
				reply.ShardData[k] = string([]byte(v))
				keyCount++
			}

			for clientId, seqNum := range kv.clientSeq {
				reply.ClientSeq[clientId] = seqNum
			}

			DPrintf("Group %d Server %d GetShard: 提供当前数据给旧配置 %d 的分片 %d 请求，总键值对: %d",
				kv.gid, kv.me, args.ConfigNum, args.Shard, keyCount)
			reply.Err = OK
			return
		}
	}

	// 如果在备份数据中找不到,检查当前数据
	// 如果请求的是更高配置号的分片,我们可能需要将当前数据提供出去
	if args.ConfigNum > kv.config.Num {
		// 配置更新迁移过程中,如果我们有该分片的数据
		if kv.config.Shards[args.Shard] == kv.gid && kv.data[args.Shard] != nil {
			// 创建当前分片数据的副本
			shardData := make(map[string]string)
			keyCount := 0

			for k, v := range kv.data[args.Shard] {
				shardData[k] = string([]byte(v))
				keyCount++
				if keyCount <= 5 {
					DPrintf("Group %d Server %d GetShard: 为更高配置 %d 提供分片 %d 的数据 - 键='%s', 值='%s'",
						kv.gid, kv.me, args.ConfigNum, args.Shard, k, v)
				}
			}

			// 提供数据和客户端序列号
			for k, v := range shardData {
				reply.ShardData[k] = v
			}
			for clientId, seqNum := range kv.clientSeq {
				reply.ClientSeq[clientId] = seqNum
			}

			// 将分片标记为迁移中状态
			kv.shardStates[args.Shard] = ShardMigrating

			// 自动备份分片数据到 outShards
			if _, exists := kv.outShards[args.ConfigNum]; !exists {
				kv.outShards[args.ConfigNum] = make(map[int]map[string]string)
			}
			kv.outShards[args.ConfigNum][args.Shard] = shardData

			DPrintf("Group %d Server %d GetShard: 为更高配置 %d 提供分片 %d 数据成功,总键值对: %d, 已备份数据",
				kv.gid, kv.me, args.ConfigNum, args.Shard, keyCount)
			reply.Err = OK
			return
		}
	}

	// 如果没有找到数据,返回错误
	DPrintf("Group %d Server %d GetShard: 找不到请求的分片 %d 配置 %d 的数据",
		kv.gid, kv.me, args.Shard, args.ConfigNum)
	reply.Err = ErrWrongGroup
}

func (kv *ShardKV) CleanShard(args *CleanShardArgs, reply *CleanShardReply) {
	// 先检查是否是领导者，不需要加锁
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      OpCleanup,
		ConfigNum: args.ConfigNum,
		Shard:     args.Shard,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 获取通知通道
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	// 清理通知通道
	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 获取或创建通知通道
func (kv *ShardKV) getNotifyCh(index int) chan OpResult {
	if _, ok := kv.notifyCh[index]; !ok {
		kv.notifyCh[index] = make(chan OpResult, 1)
	}
	return kv.notifyCh[index]
}

// 后台协程：应用日志
func (kv *ShardKV) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()

			index := msg.CommandIndex
			op := msg.Command.(Op)
			result := OpResult{
				ClientId: op.ClientId,
				SeqNum:   op.SeqNum,
			}

			if !kv.ReplayComplete {
				if op.ClientId != 0 {
					kv.InitialClients[op.ClientId] = true
				}
			}

			if index <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			isDup := false
			isClientOp := (op.Type == OpGet || op.Type == OpPut || op.Type == OpAppend) && op.ClientId != 0
			if isClientOp {
				lastSeq, exist := kv.clientSeq[op.ClientId]
				isDup = exist && op.SeqNum <= lastSeq
			}

			if !isDup {
				switch op.Type {
				case OpGet:
					result = kv.applyGet(op)
				case OpPut:
					result = kv.applyPut(op)
				case OpAppend:
					result = kv.applyAppend(op)
				case OpConfig:
					result = kv.applyConfig(op)
				case OpTransfer:
					DPrintf("Group %d Server %d applyLoop: WARN - received deprecated OpTransfer", kv.gid, kv.me)
					result = kv.applyTransfer(op)
				case OpCleanup:
					result = kv.applyCleanup(op)
				case OpInstallShards:
					result = kv.applyInstallShards(op)
				default:
					DPrintf("Group %d Server %d applyLoop: ERROR - unknown op type %s", kv.gid, kv.me, op.Type)
					result.Err = ErrWrongLeader
				}

				if isClientOp && (op.Type == OpPut || op.Type == OpAppend) && result.Err == OK {
					kv.clientSeq[op.ClientId] = op.SeqNum
				}
			} else {
				result.Err = OK
				if op.Type == OpGet {
					shard := key2shard(op.Key)
					if kv.isShardAvailable(shard) {
						if value, exists := kv.data[shard][op.Key]; exists {
							result.Value = value
						} else {
							result.Err = ErrNoKey
						}
					} else {
						result.Err = ErrWrongGroup
					}
				} else {
				}
			}

			kv.lastApplied = index

			if !kv.ReplayComplete && index > kv.lastIncludeIndex {
				commitReplayComplete := false
				if kv.lastApplied > kv.lastIncludeIndex {
					commitReplayComplete = true
					DPrintf("Group %d Server %d prepared to mark replay as complete, lastApplied %d > lastIncludeIndex %d",
						kv.gid, kv.me, kv.lastApplied, kv.lastIncludeIndex)
				}

				if commitReplayComplete {
					for shard := 0; shard < shardctrler.NShards; shard++ {
						if kv.config.Shards[shard] == kv.gid {
							if _, ok := kv.data[shard]; !ok {
								kv.data[shard] = make(map[string]string)
								DPrintf("Group %d Server %d REPLAY initialized empty shard %d during completion",
									kv.gid, kv.me, shard)
							}
						}
					}

					totalKeys := countTotalKeys(kv.data)
					DPrintf("Group %d Server %d REPLAY COMPLETED at index %d (lastIncludeIndex: %d, lastApplied: %d), total keys: %d, clientSeq size: %d, config: %+v",
						kv.gid, kv.me, index, kv.lastIncludeIndex, kv.lastApplied, totalKeys, len(kv.clientSeq), kv.config)

					clientCount := 0
					for clientId, seqNum := range kv.clientSeq {
						if clientCount < 10 {
							DPrintf("Group %d Server %d client %d last seq: %d after replay",
								kv.gid, kv.me, clientId, seqNum)
							clientCount++
						} else {
							break
						}
					}

					kv.ReplayComplete = true
					close(kv.ReplayDone)
					DPrintf("Group %d Server %d closed replayDone channel", kv.gid, kv.me)
				}
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				if ch, ok := kv.notifyCh[index]; ok {
					select {
					case ch <- result:
					default:
					}
				}
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)

				totalKeys := 0
				activeShards := 0
				for _, shardData := range kv.data {
					if len(shardData) > 0 {
						activeShards++
						totalKeys += len(shardData)
					}
				}

				// 记录待接收的分片数
				waitingShardCount := len(kv.waitingShards)

				// 记录待发送的分片数
				outShardCount := 0
				for _, configShards := range kv.outShards {
					outShardCount += len(configShards)
				}

				// 记录客户端会话信息
				clientCount := len(kv.clientSeq)

				DPrintf("Group %d Server %d snapshot: creating at index %d, active shards: %d, keys: %d, "+
					"waiting: %d, outgoing: %d, clients: %d, config: %d",
					kv.gid, kv.me, index, activeShards, totalKeys, waitingShardCount,
					outShardCount, clientCount, kv.config.Num)

				// 创建快照状态对象，只包含需要序列化的字段
				state := SnapshotState{
					Data:               kv.data,
					ClientSeq:          kv.clientSeq,
					Config:             kv.config,
					PrevConfig:         kv.prevConfig,
					WaitingShards:      kv.waitingShards,
					OutShards:          kv.outShards,
					LastIncludeIndex:   kv.lastIncludeIndex,
					InitialClients:     kv.InitialClients,
					ShardStates:        kv.shardStates,
					LastConfigTimeUnix: kv.LastConfigTime.Unix(),
					ReplayComplete:     kv.ReplayComplete,
				}

				if e.Encode(state) != nil {
					DPrintf("Group %v Server %v ERROR encoding snapshot", kv.gid, kv.me)
				} else {
					snapshot := w.Bytes()
					kv.rf.Snapshot(index, snapshot)
					kv.lastIncludeIndex = index
					DPrintf("Group %v Server %v created snapshot at index %v, size: %v bytes",
						kv.gid, kv.me, index, len(snapshot))
				}
			}

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.lastApplied < msg.SnapshotIndex {
				DPrintf("Group %d Server %d installing snapshot at index %d, size: %d bytes",
					kv.gid, kv.me, msg.SnapshotIndex, len(msg.Snapshot))

				snapshot := msg.Snapshot
				if snapshot != nil && len(snapshot) > 0 {
					r := bytes.NewBuffer(snapshot)
					d := labgob.NewDecoder(r)

					var state SnapshotState
					if d.Decode(&state) != nil {
						DPrintf("Group %d Server %d ERROR decoding snapshot", kv.gid, kv.me)
					} else {
						kv.data = state.Data
						kv.clientSeq = state.ClientSeq
						kv.config = state.Config
						kv.prevConfig = state.PrevConfig
						kv.waitingShards = state.WaitingShards
						kv.outShards = state.OutShards
						kv.lastIncludeIndex = state.LastIncludeIndex
						kv.lastApplied = state.LastIncludeIndex
						kv.InitialClients = state.InitialClients
						kv.shardStates = state.ShardStates
						kv.LastConfigTime = time.Unix(state.LastConfigTimeUnix, 0)
						kv.ReplayComplete = state.ReplayComplete

						if kv.ReplayComplete {
							select {
							case <-kv.ReplayDone:
								// 通道已经关闭，不需要再次关闭
							default:
								close(kv.ReplayDone)
							}
						}

						DPrintf("Group %d Server %d snapshot applied, index %d, config %d, shards: %d, keys: %d",
							kv.gid, kv.me, state.LastIncludeIndex, kv.config.Num, len(kv.data),
							func() int {
								count := 0
								for _, shard := range kv.data {
									count += len(shard)
								}
								return count
							}())
					}
				}
			}
			kv.mu.Unlock()
		}
	}
}

// applyGet applies a Get operation to the state machine
func (kv *ShardKV) applyGet(op Op) OpResult {
	result := OpResult{
		ClientId: op.ClientId,
		SeqNum:   op.SeqNum,
	}

	shard := key2shard(op.Key)

	// 检查分片是否可用
	if !kv.isShardAvailable(shard) {
		result.Err = ErrWrongGroup
		return result
	}

	// 检查分片数据是否存在
	if _, ok := kv.data[shard]; !ok {
		kv.data[shard] = make(map[string]string)
	}

	if value, exists := kv.data[shard][op.Key]; exists {
		result.Value = value
		result.Err = OK
		DPrintf("Group %d GET key %s = %s (replay: %v)",
			kv.gid, op.Key, value, !kv.ReplayComplete)
	} else {
		result.Err = ErrNoKey
		DPrintf("Group %d GET key %s not found (replay: %v)",
			kv.gid, op.Key, !kv.ReplayComplete)
	}

	return result
}

// applyPut applies a Put operation to the state machine
func (kv *ShardKV) applyPut(op Op) OpResult {
	result := OpResult{
		ClientId: op.ClientId,
		SeqNum:   op.SeqNum,
	}

	shard := key2shard(op.Key)

	// 检查分片是否可用
	if !kv.isShardAvailable(shard) {
		result.Err = ErrWrongGroup
		return result
	}

	// 检查分片数据是否存在
	if _, ok := kv.data[shard]; !ok {
		kv.data[shard] = make(map[string]string)
	}

	// 判断是否是重复请求
	if kv.isDuplicateRequest(op.ClientId, op.SeqNum) && kv.ReplayComplete {
		result.Err = OK
		return result
	}

	// 执行Put操作
	kv.data[shard][op.Key] = op.Value
	//DPrintf("Group %d PUT key %s = (len=%d) (replay: %v)",
	//	kv.gid, op.Key, len(op.Value), !kv.ReplayComplete)

	kv.clientSeq[op.ClientId] = op.SeqNum
	result.Err = OK
	return result
}

// applyAppend applies an Append operation to the state machine
func (kv *ShardKV) applyAppend(op Op) OpResult {
	result := OpResult{
		ClientId: op.ClientId,
		SeqNum:   op.SeqNum,
	}

	shard := key2shard(op.Key)

	// 检查分片是否可用
	if !kv.isShardAvailable(shard) {
		result.Err = ErrWrongGroup
		return result
	}

	// 检查分片数据是否存在
	if _, ok := kv.data[shard]; !ok {
		kv.data[shard] = make(map[string]string)
	}

	// 判断是否是重复请求
	if kv.isDuplicateRequest(op.ClientId, op.SeqNum) && kv.ReplayComplete {
		result.Err = OK
		return result
	}

	// 记录原始值的长度，用于日志记录
	oldValue := ""
	oldLen := 0
	if val, exists := kv.data[shard][op.Key]; exists {
		oldValue = val
		oldLen = len(val)
	}

	// 执行Append操作：将新值追加到原值后面
	newValue := oldValue + op.Value
	kv.data[shard][op.Key] = newValue

	DPrintf("Group %d Server %d applyAppend: 键 %s 追加操作 - 原始值长度:%d, 附加值长度:%d, 新值长度:%d",
		kv.gid, kv.me, op.Key, oldLen, len(op.Value), len(newValue))

	// 如果新值超过一定长度，只记录部分内容到日志中
	if len(newValue) > 100 {
		DPrintf("Group %d Server %d applyAppend: 键 %s 值现在为 %s...(截断，完整长度=%d)",
			kv.gid, kv.me, op.Key, newValue[:50], len(newValue))
	} else {
		DPrintf("Group %d Server %d applyAppend: 键 %s 值现在为 %s (长度=%d)",
			kv.gid, kv.me, op.Key, newValue, len(newValue))
	}

	// 更新客户端序列号
	kv.clientSeq[op.ClientId] = op.SeqNum
	result.Err = OK
	return result
}

// 应用配置更新
func (kv *ShardKV) applyConfig(op Op) OpResult {
	result := OpResult{
		ClientId: op.ClientId,
		SeqNum:   op.SeqNum,
		Err:      OK,
	}

	if op.Config.Num <= kv.config.Num {
		//DPrintf("Group %d Server %d applyConfig: ignoring outdated config %d, current is %d",
		//	kv.gid, kv.me, op.Config.Num, kv.config.Num)
		return result
	}

	// 确保配置是连续更新的
	if op.Config.Num > kv.config.Num+1 {
		//DPrintf("Group %d Server %d applyConfig: 检测到配置跳跃 from %d to %d，尝试获取中间配置",
		//	kv.gid, kv.me, kv.config.Num, op.Config.Num)

		// 递归地应用中间配置
		for i := kv.config.Num + 1; i < op.Config.Num; i++ {
			midConfig := kv.mck.Query(i)
			midOp := Op{
				Type:   OpConfig,
				Config: midConfig,
			}
			midResult := kv.applyConfig(midOp)
			if midResult.Err != OK {
				//DPrintf("Group %d Server %d applyConfig: 应用中间配置 %d 失败: %v",
				//	kv.gid, kv.me, i, midResult.Err)
				result.Err = midResult.Err
				return result
			}
		}
		// 继续应用当前配置
	}

	// 确保没有未完成的分片迁移
	if len(kv.waitingShards) > 0 {
		//DPrintf("Group %d Server %d applyConfig: ERROR - attempting to update config with %d waiting shards",
		//	kv.gid, kv.me, len(kv.waitingShards))
		result.Err = ErrWrongLeader
		return result
	}

	//DPrintf("Group %d Server %d applyConfig: applying new config %d from %d",
	//	kv.gid, kv.me, op.Config.Num, kv.config.Num)

	// 保存当前配置为前一个配置
	oldConfig := kv.config
	kv.prevConfig = oldConfig

	// 为准备离开的分片创建备份
	for shard := 0; shard < shardctrler.NShards; shard++ {
		// 只处理原本属于当前组的分片
		if oldConfig.Shards[shard] == kv.gid {
			// 如果在新配置中分片所属组发生了变化
			if op.Config.Shards[shard] != kv.gid {
				//DPrintf("Group %d Server %d applyConfig: shard %d moving from us to group %d in config %d",
				//	kv.gid, kv.me, shard, op.Config.Shards[shard], op.Config.Num)

				// 标记分片为迁移中状态
				kv.shardStates[shard] = ShardMigrating

				// 该分片需要发送给其他组
				configNum := op.Config.Num
				if _, exists := kv.outShards[configNum]; !exists {
					kv.outShards[configNum] = make(map[int]map[string]string)
				}

				// 复制分片数据
				if _, exists := kv.data[shard]; exists {
					shardCopy := make(map[string]string)
					keyCount := 0
					totalKeys := len(kv.data[shard])

					DPrintf("Group %d Server %d applyConfig: 开始备份分片 %d 的数据，原始数据大小: %d",
						kv.gid, kv.me, shard, totalKeys)

					for k, v := range kv.data[shard] {
						shardCopy[k] = v
						DPrintf("Group %d Server %d applyConfig: 备份分片 %d 数据 - 键='%s', 值='%s' (长度=%d)",
							kv.gid, kv.me, shard, k, v, len(v))
						keyCount++
					}
					kv.outShards[configNum][shard] = shardCopy
					DPrintf("Group %d Server %d applyConfig: 完成分片 %d 备份，总键值对: %d，备份后大小: %d",
						kv.gid, kv.me, shard, keyCount, len(shardCopy))
				} else {
					// 如果分片数据不存在，创建空数据
					kv.outShards[configNum][shard] = make(map[string]string)
					//DPrintf("Group %d Server %d applyConfig: created empty backup for shard %d in config %d",
					//	kv.gid, kv.me, shard, configNum)
				}

				// 从当前数据中删除分片
				delete(kv.data, shard)
				//DPrintf("Group %d Server %d applyConfig: removed shard %d from current data after backup",
				//	kv.gid, kv.me, shard)
			} else {
				//DPrintf("Group %d Server %d applyConfig: keeping shard %d as it remains assigned to us",
				//	kv.gid, kv.me, shard)
			}
		}
	}

	// 处理新配置中属于当前组的分片
	newWaitingShards := 0
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if op.Config.Shards[shard] == kv.gid {
			// 如果分片之前就属于当前组，不需要特殊处理
			if oldConfig.Num > 0 && oldConfig.Shards[shard] == kv.gid {
				// 确保分片状态为正常
				kv.shardStates[shard] = ShardNormal
				//DPrintf("Group %d Server %d applyConfig: keeps shard %d as it was already owned",
				//	kv.gid, kv.me, shard)
				continue
			}

			// 这个分片现在属于我们，但需要从其他组获取
			if oldConfig.Num == 0 || oldConfig.Shards[shard] == 0 {
				// 对于初始配置或无所有者的分片，直接创建空分片
				if _, ok := kv.data[shard]; !ok {
					kv.data[shard] = make(map[string]string)
				}
				kv.shardStates[shard] = ShardNormal
				//DPrintf("Group %d Server %d applyConfig: created empty shard %d for initial config",
				//	kv.gid, kv.me, shard)
			} else {
				// 否则需要等待从其他组获取数据
				kv.waitingShards[shard] = true
				kv.shardStates[shard] = ShardWaiting
				newWaitingShards++
			}
		}
	}

	// 更新当前配置
	kv.config = op.Config
	kv.LastConfigTime = time.Now() // 更新配置时间

	//DPrintf("Group %d Server %d applyConfig: applied new config %d, waiting for %d shards, outgoing shards: %d",
	//	kv.gid, kv.me, op.Config.Num, newWaitingShards, len(kv.outShards))

	return result
}

// 应用接收分片操作
func (kv *ShardKV) applyTransfer(op Op) OpResult {
	result := OpResult{
		ClientId: op.ClientId,
		SeqNum:   op.SeqNum,
		Err:      OK,
	}

	// 检查配置号是否匹配
	if op.ConfigNum != kv.config.Num {
		result.Err = ErrWrongGroup
		return result
	}

	// 创建分片存储（如果不存在）
	if _, exists := kv.data[op.Shard]; !exists {
		kv.data[op.Shard] = make(map[string]string)
	}

	// 合并分片数据
	singleShardData := make(map[string]string)
	if shardDataMap, ok := op.ShardData[op.Shard]; ok {
		for k, v := range shardDataMap {
			singleShardData[k] = v
		}
	}

	// 将数据添加到分片中
	for k, v := range singleShardData {
		kv.data[op.Shard][k] = v
	}

	// 合并客户端序列号（可能是空的）
	if op.ClientSeq != nil {
		for cid, seq := range op.ClientSeq {
			if lastSeq, ok := kv.clientSeq[cid]; !ok || seq > lastSeq {
				kv.clientSeq[cid] = seq
			}
		}
	}

	// 标记该分片已接收完成
	delete(kv.waitingShards, op.Shard)

	DPrintf("[Server %d Group %d] successfully applied transfer for shard %d config %d, keys: %d",
		kv.me, kv.gid, op.Shard, op.ConfigNum, len(kv.data[op.Shard]))

	return result
}

// 应用分片清理
func (kv *ShardKV) applyCleanup(op Op) OpResult {
	result := OpResult{
		ClientId: op.ClientId,
		SeqNum:   op.SeqNum,
		Err:      OK,
	}

	// 清理指定配置的指定分片
	if outShards, ok := kv.outShards[op.ConfigNum]; ok {
		delete(outShards, op.Shard)

		// 如果该配置下没有分片了，删除整个配置
		if len(outShards) == 0 {
			delete(kv.outShards, op.ConfigNum)
		}

		// 将分片状态重置为正常
		// 注意：只有在确认分片已经成功迁移后才重置状态
		if kv.shardStates[op.Shard] == ShardMigrating {
			kv.shardStates[op.Shard] = ShardNormal
			DPrintf("Group %d Server %d applyCleanup: reset shard %d state to Normal after cleanup",
				kv.gid, kv.me, op.Shard)
		}
	}

	return result
}

// 拉取并处理配置更新
func (kv *ShardKV) configurationLoop() {
	for !kv.killed() {
		// 只有领导者才检查配置更新
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 等待日志重放完成
		if !kv.ReplayComplete {
			//DPrintf("Group %d Server %d configurationLoop: waiting for log replay to complete",
			//	kv.gid, kv.me)
			select {
			case <-kv.ReplayDone:
				DPrintf("Group %d Server %d configurationLoop: replay completed, continuing with config checking",
					kv.gid, kv.me)
			}
		}

		kv.mu.Lock()
		// 检查是否有等待的分片或待清理的分片
		canUpdate := true
		waitingCount := len(kv.waitingShards)
		outShardsCount := 0

		for _, shards := range kv.outShards {
			outShardsCount += len(shards)
		}

		if waitingCount > 0 || outShardsCount > 0 {
			canUpdate = false
			//DPrintf("Group %d Server %d configurationLoop: cannot update config while waiting for %d shards and having %d outgoing shards",
			//	kv.gid, kv.me, waitingCount, outShardsCount)
		}

		currentConfigNum := kv.config.Num
		kv.mu.Unlock()

		if !canUpdate {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 查询下一个配置
		nextConfig := kv.mck.Query(currentConfigNum + 1)

		if nextConfig.Num == currentConfigNum+1 {
			DPrintf("Group %d Server %d configurationLoop: found new config %d, submitting to Raft",
				kv.gid, kv.me, nextConfig.Num)

			// 发现新配置，提交到 Raft
			op := Op{
				Type:   OpConfig,
				Config: nextConfig,
			}

			index, term, isLeader := kv.rf.Start(op)
			if !isLeader {
				DPrintf("Group %d Server %d configurationLoop: lost leadership before submitting config update",
					kv.gid, kv.me)
			} else {
				DPrintf("Group %d Server %d configurationLoop: submitted config update to index %d, term %d",
					kv.gid, kv.me, index, term)

				// 短暂等待，给Raft时间处理这个命令
				time.Sleep(50 * time.Millisecond)

				// 再次检查是否还是领导者
				if _, stillLeader := kv.rf.GetState(); stillLeader {
					// 检查配置是否已应用
					kv.mu.Lock()
					configApplied := kv.config.Num == nextConfig.Num
					kv.mu.Unlock()

					if configApplied {
						DPrintf("Group %d Server %d configurationLoop: new config %d successfully applied",
							kv.gid, kv.me, nextConfig.Num)

						// 广播新配置给其他组
						kv.mu.Lock()
						broadcastConfig := kv.config // 复制当前配置
						kv.mu.Unlock()
						kv.broadcastConfig(broadcastConfig)
					} else {
						DPrintf("Group %d Server %d configurationLoop: waiting for config %d to be applied",
							kv.gid, kv.me, nextConfig.Num)
					}
				}
			}
		} else if nextConfig.Num > currentConfigNum+1 {
			// 发现跳跃的配置，这可能表明出现问题
			DPrintf("Group %d Server %d configurationLoop: WARNING: detected config jump from %d to %d",
				kv.gid, kv.me, currentConfigNum, nextConfig.Num)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// isDuplicateRequest 检查给定的客户端ID和序列号是否是重复请求
func (kv *ShardKV) isDuplicateRequest(clientId int64, seqNum int) bool {
	lastSeq, ok := kv.clientSeq[clientId]
	return ok && seqNum <= lastSeq
}

// applyInstallShards applies an InstallShards operation to the state machine
func (kv *ShardKV) applyInstallShards(op Op) OpResult {
	result := OpResult{
		ClientId: op.ClientId,
		SeqNum:   op.SeqNum,
		Err:      OK,
	}

	// 确保配置号匹配
	if op.ConfigNum != kv.config.Num {
		result.Err = ErrWrongGroup
		DPrintf("Group %d rejected shard installation due to config mismatch: current %d, requested %d",
			kv.gid, kv.config.Num, op.ConfigNum)
		return result
	}

	// 创建临时映射存储新数据
	newData := make(map[int]map[string]string)
	newClientSeq := make(map[int64]int)

	// 计算总数据大小，用于日志记录
	totalDataSize := 0
	totalKeyCount := 0
	for _, shard := range op.Shards {
		if shardData, ok := op.ShardData[shard]; ok {
			totalKeyCount += len(shardData)
			for _, v := range shardData {
				totalDataSize += len(v)
			}
		}
	}

	// 在日志重放过程中记录更详细的信息
	if !kv.ReplayComplete {
		DPrintf("Group %d Server %d REPLAY: installing shards %v during replay, config: %d, total keys: %d, data size: %d bytes",
			kv.gid, kv.me, op.Shards, op.ConfigNum, totalKeyCount, totalDataSize)
	} else {
		DPrintf("Group %d Server %d applyInstallShards: 开始安装分片 %v, 配置: %d, 总键值对: %d, 数据大小: %d字节",
			kv.gid, kv.me, op.Shards, op.ConfigNum, totalKeyCount, totalDataSize)
	}

	// 首先准备所有数据
	for _, shard := range op.Shards {
		// 检查是否在等待队列中
		if _, waiting := kv.waitingShards[shard]; waiting {
			// 准备新的分片数据
			if shardData, ok := op.ShardData[shard]; ok {
				if _, exists := newData[shard]; !exists {
					newData[shard] = make(map[string]string)
				}

				keysAdded := 0
				dataBytesSize := 0

				for k, v := range shardData {
					newValue := string([]byte(v))
					newData[shard][k] = newValue
					keysAdded++
					dataBytesSize += len(newValue)

					// 记录样本数据
					if keysAdded <= 5 {
						DPrintf("Group %d Server %d shard %d 样本数据: 键='%s' -> 值='%s' (长度=%d)",
							kv.gid, kv.me, shard, k, v, len(v))
					}
				}

				DPrintf("Group %d Server %d applyInstallShards: 分片 %d 数据统计 - 新增键:%d, 数据大小:%d字节",
					kv.gid, kv.me, shard, keysAdded, dataBytesSize)
			} else {
				// 如果没有数据，创建空映射
				newData[shard] = make(map[string]string)
				DPrintf("Group %d Server %d applyInstallShards: 分片 %d 没有收到数据",
					kv.gid, kv.me, shard)
			}
		}
	}

	// 更新客户端序列号
	clientSeqUpdates := 0
	for clientId, seqNum := range op.ClientSeq {
		lastSeq, exists := kv.clientSeq[clientId]
		if !exists || seqNum > lastSeq {
			newClientSeq[clientId] = seqNum
			clientSeqUpdates++
		}
	}

	// 原子性地应用所有更改
	for shard, data := range newData {
		kv.data[shard] = data
		delete(kv.waitingShards, shard)
		kv.shardStates[shard] = ShardNormal

		DPrintf("Group %d Server %d applyInstallShards: 分片 %d 安装完成，当前键值对数量: %d",
			kv.gid, kv.me, shard, len(data))
	}

	// 更新客户端序列号
	for clientId, seqNum := range newClientSeq {
		kv.clientSeq[clientId] = seqNum
	}

	// 检查是否还有更多的分片待安装
	remainingWaitingShards := len(kv.waitingShards)
	if remainingWaitingShards > 0 {
		DPrintf("Group %d Server %d applyInstallShards: 仍有 %d 个分片等待安装",
			kv.gid, kv.me, remainingWaitingShards)
	} else {
		DPrintf("Group %d Server %d applyInstallShards: 所有分片安装完成，配置 %d 的分片迁移完成",
			kv.gid, kv.me, kv.config.Num)
	}

	return result
}

// 读取并打印Raft持久化的日志内容
func PrintRaftLogs(persister *raft.Persister, gid int, me int) {
	state := persister.ReadRaftState()
	if len(state) == 0 {
		DPrintf("Group %d Server %d STARTUP: No Raft state found", gid, me)
		return
	}

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []raft.LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	var lastApplied int
	var commitIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Group %d Server %d STARTUP ERROR: Failed to decode basic Raft state", gid, me)
		return
	}

	// 尝试解码快照相关字段，但允许它们可能不存在
	hasSnapshotInfo := true
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastApplied) != nil {
		hasSnapshotInfo = false
	}

	// 可选地读取commitIndex
	hasCommitIndex := true
	if d.Decode(&commitIndex) != nil {
		hasCommitIndex = false
	}

	// 输出基本Raft状态信息
	infoStr := fmt.Sprintf("Group %d Server %d STARTUP: Raft state - currentTerm: %d, votedFor: %d, log entries: %d",
		gid, me, currentTerm, votedFor, len(log))

	if hasSnapshotInfo {
		infoStr += fmt.Sprintf(", lastIncludedIndex: %d, lastIncludedTerm: %d, lastApplied: %d",
			lastIncludedIndex, lastIncludedTerm, lastApplied)
	}

	if hasCommitIndex {
		infoStr += fmt.Sprintf(", commitIndex: %d", commitIndex)
	}

	DPrintf("%s", infoStr)

	// 打印一下
	if len(log) > 0 {
		DPrintf("Group %d Server %d STARTUP: Printing Raft log entries (showing up to 10 entries from start and end):", gid, me)
		for i, entry := range log {
			if i < 10 || i >= len(log)-10 {
				command, ok := entry.Command.(Op)
				if ok {
					cmdDetails := fmt.Sprintf("Type=%s", command.Type)
					if command.Key != "" {
						cmdDetails += fmt.Sprintf(", Key=%s", command.Key)
					}
					if command.Value != "" {
						cmdDetails += fmt.Sprintf(", Value=%s", command.Value)
					}
					if command.ClientId != 0 {
						cmdDetails += fmt.Sprintf(", ClientId=%d", command.ClientId)
					}
					if command.SeqNum != 0 {
						cmdDetails += fmt.Sprintf(", SeqNum=%d", command.SeqNum)
					}
					if command.Type == OpConfig && command.Config.Num != 0 {
						cmdDetails += fmt.Sprintf(", ConfigNum=%d", command.Config.Num)
					}
					if command.Type == OpTransfer || command.Type == OpCleanup {
						cmdDetails += fmt.Sprintf(", Shard=%d, ConfigNum=%d", command.Shard, command.ConfigNum)
					}
					if command.Type == OpInstallShards {
						cmdDetails += fmt.Sprintf(", Shards=%v, ConfigNum=%d", command.Shards, command.ConfigNum)
					}

					// 计算绝对索引
					absoluteIndex := i
					if hasSnapshotInfo && lastIncludedIndex > 0 {
						absoluteIndex = lastIncludedIndex + i
					}

					DPrintf("Group %d Server %d STARTUP: Log[%d] Term=%d Command={%s}",
						gid, me, absoluteIndex, entry.Term, cmdDetails)
				} else if entry.Command != nil {
					DPrintf("Group %d Server %d STARTUP: Log[%d] Term=%d Command=(type: %T, not Op type)",
						gid, me, i, entry.Term, entry.Command)
				} else {
					DPrintf("Group %d Server %d STARTUP: Log[%d] Term=%d Command=nil",
						gid, me, i, entry.Term)
				}
			} else if i == 10 && len(log) > 20 {
				DPrintf("Group %d Server %d STARTUP: ... (omitting %d entries) ...",
					gid, me, len(log)-20)
			}
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// Your initialization code here.
	// 注册类型
	labgob.Register(Op{})
	labgob.Register(OpResult{})
	labgob.Register(raft.LogEntry{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(GetShardArgs{})
	labgob.Register(GetShardReply{})
	labgob.Register(CleanShardArgs{})
	labgob.Register(CleanShardReply{})
	labgob.Register(NotifyConfigArgs{})
	labgob.Register(NotifyConfigReply{})
	labgob.Register(map[int]map[string]string{})
	labgob.Register(map[int64]int{})
	labgob.Register(map[int]bool{})
	labgob.Register(map[int]map[int]map[string]string{})
	labgob.Register(map[int64]bool{})
	labgob.Register(map[int]int{})
	labgob.Register([]int{})
	labgob.Register(SnapshotState{})
	labgob.Register(Err(""))

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.config = shardctrler.Config{Num: 0}
	kv.prevConfig = shardctrler.Config{Num: 0}
	kv.lastApplied = 0
	kv.lastIncludeIndex = 0
	kv.LastConfigTime = time.Now() // 初始化配置时间

	kv.clientSeq = make(map[int64]int)
	kv.data = make(map[int]map[string]string)
	kv.waitingShards = make(map[int]bool)
	kv.outShards = make(map[int]map[int]map[string]string)
	kv.notifyCh = make(map[int]chan OpResult)

	kv.ReplayComplete = false
	kv.InitialClients = make(map[int64]bool)
	kv.ReplayDone = make(chan struct{})

	kv.shardStates = make(map[int]int)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardStates[i] = ShardNormal
	}

	snapshot := persister.ReadSnapshot()
	raftStateSize := persister.RaftStateSize()

	needLogReplay := false
	startReplayIndex := 0

	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)

		var data map[int]map[string]string
		var clientSeq map[int64]int
		var config shardctrler.Config
		var prevConfig shardctrler.Config
		var waitingShards map[int]bool
		var outShards map[int]map[int]map[string]string
		var lastIncludeIndex int
		var InitialClients map[int64]bool
		var shardStates map[int]int
		var lastConfigTimeUnix int64

		decodeErr := false
		if d.Decode(&data) != nil ||
			d.Decode(&clientSeq) != nil ||
			d.Decode(&config) != nil ||
			d.Decode(&prevConfig) != nil ||
			d.Decode(&waitingShards) != nil ||
			d.Decode(&outShards) != nil ||
			d.Decode(&lastIncludeIndex) != nil ||
			d.Decode(&InitialClients) != nil ||
			d.Decode(&shardStates) != nil {
			decodeErr = true
		}

		if !decodeErr {
			err := d.Decode(&lastConfigTimeUnix)
			if err != nil {
				var lastConfigTime time.Time
				d.Decode(&lastConfigTime)

				if !lastConfigTime.IsZero() {
					lastConfigTimeUnix = lastConfigTime.Unix()
				} else {
					lastConfigTimeUnix = time.Now().Unix()
				}
			}
		}

		if !decodeErr {
			kv.data = data
			kv.clientSeq = clientSeq
			kv.config = config
			kv.prevConfig = prevConfig
			kv.waitingShards = waitingShards
			kv.outShards = outShards
			kv.lastIncludeIndex = lastIncludeIndex
			kv.lastApplied = lastIncludeIndex
			kv.InitialClients = InitialClients
			kv.LastConfigTime = time.Unix(lastConfigTimeUnix, 0)
			if shardStates == nil || len(shardStates) == 0 {
				// 如果快照中没有分片状态，初始化为默认值
				kv.shardStates = make(map[int]int)
				for i := 0; i < shardctrler.NShards; i++ {
					// 根据分片所有权和等待状态设置初始值
					if _, waiting := waitingShards[i]; waiting {
						kv.shardStates[i] = ShardWaiting
					} else if _, hasData := data[i]; hasData {
						kv.shardStates[i] = ShardNormal
					} else {
						// 检查是否在outShards中
						isOutgoing := false
						for _, shards := range outShards {
							if _, found := shards[i]; found {
								isOutgoing = true
								kv.shardStates[i] = ShardMigrating
								break
							}
						}
						if !isOutgoing {
							kv.shardStates[i] = ShardNormal
						}
					}
				}
				DPrintf("Group %d Server %d STARTUP: 快照中无分片状态信息，已根据分片所有权重建分片状态",
					kv.gid, kv.me)
			} else {
				kv.shardStates = shardStates
			}

			needLogReplay = true
			startReplayIndex = lastIncludeIndex + 1
			kv.ReplayComplete = false

		}
	} else if raftStateSize > 0 {
		needLogReplay = true
		startReplayIndex = 1
		kv.ReplayComplete = false
		DPrintf("Group %d Server %d STARTUP: No snapshot but have Raft state, will replay from index 1",
			gid, me)
	} else {
		kv.ReplayComplete = true
		close(kv.ReplayDone)
		DPrintf("Group %d Server %d STARTUP: Fresh server with no state, replay marked as complete",
			gid, me)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 启动后台协程
	go kv.applyLoop()
	go kv.configurationLoop()
	go kv.pullShardsLoop()
	go kv.validateConfigState() // 启动配置状态验证

	if needLogReplay {
		go func() {
			time.Sleep(1000*time.Millisecond + time.Duration(rand.Intn(50))*time.Millisecond)
			DPrintf("Group %d Server %d STARTUP: Requesting log replay from index %d",
				gid, me, startReplayIndex)
			kv.rf.RequestLogReplay(startReplayIndex, -1)
		}()
	}

	return kv
}

func (kv *ShardKV) broadcastConfig(config shardctrler.Config) {
	// 向其他组广播配置更新
	for gid, servers := range config.Groups {
		if gid != kv.gid {
			go func(targetGid int, targetServers []string) {
				// 尝试向组中的每个服务器发送通知
				for _, srv := range targetServers {
					args := &NotifyConfigArgs{
						ConfigNum: config.Num,
						Config:    config,
					}
					var reply NotifyConfigReply
					serverEnd := kv.make_end(srv)
					ok := serverEnd.Call("ShardKV.NotifyConfigUpdate", args, &reply)
					if ok && reply.Err == OK {
						DPrintf("Group %d Server %d 成功通知 Group %d 配置更新到 %d",
							kv.gid, kv.me, targetGid, config.Num)
						break // 成功通知一个服务器即可
					}
				}
			}(gid, servers)
		}
	}
}

// 处理配置更新通知
func (kv *ShardKV) NotifyConfigUpdate(args *NotifyConfigArgs, reply *NotifyConfigReply) {
	// 检查是否是领导者
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// 检查通知的配置是否比当前配置更新
	if args.ConfigNum > kv.config.Num {
		DPrintf("Group %d Server %d NotifyConfigUpdate: 收到来自其他组的配置更新通知，配置号: %d (当前配置: %d)",
			kv.gid, kv.me, args.ConfigNum, kv.config.Num)

		// 如果新配置比当前配置高两个或以上版本，我们需要尝试获取中间配置
		if args.ConfigNum > kv.config.Num+1 {
			kv.mu.Unlock()

			// 异步触发配置更新
			go func() {
				for i := kv.config.Num + 1; i <= args.ConfigNum; i++ {
					cfg := kv.mck.Query(i)
					if cfg.Num == i {
						op := Op{
							Type:   OpConfig,
							Config: cfg,
						}
						kv.rf.Start(op)
						time.Sleep(100 * time.Millisecond)
					}
				}
			}()

			reply.Err = OK
			return
		}

		// 如果只比当前配置高一个版本，直接提交到Raft
		op := Op{
			Type:   OpConfig,
			Config: args.Config,
		}
		kv.mu.Unlock()

		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		DPrintf("Group %d Server %d NotifyConfigUpdate: 提交配置更新到index %d, term %d",
			kv.gid, kv.me, index, term)

		reply.Err = OK
	} else {
		// 配置已经是最新的或比通知的配置更新
		DPrintf("Group %d Server %d NotifyConfigUpdate: 配置已经是最新的或比通知的配置更新",
			kv.gid, kv.me)
		reply.Err = OK
		kv.mu.Unlock()
	}
}

// 定期验证配置状态
func (kv *ShardKV) validateConfigState() {
	checkInterval := 5 * time.Second

	for !kv.killed() {
		// 只检查leader状态
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(1 * time.Second)
			continue
		}

		// 等待日志重放完成
		if !kv.ReplayComplete {
			select {
			case <-kv.ReplayDone:
			case <-time.After(1 * time.Second):
				continue
			}
		}

		kv.mu.Lock()
		currentConfig := kv.config
		waitingCount := len(kv.waitingShards)
		outgoingCount := 0
		for _, shards := range kv.outShards {
			outgoingCount += len(shards)
		}
		configTime := kv.LastConfigTime
		kv.mu.Unlock()

		stuckWaiting := false
		configStuck := false

		if waitingCount > 0 && configTime.Add(20*time.Second).Before(time.Now()) {
			stuckWaiting = true
			DPrintf("Group %d Server %d validateConfigState: stuck waiting for %d shards for over 20 seconds, checking latest config",
				kv.gid, kv.me, waitingCount)
		}

		if configTime.Add(30 * time.Second).Before(time.Now()) {
			configStuck = true
			DPrintf("Group %d Server %d validateConfigState: no config updates for over 30 seconds, checking latest config",
				kv.gid, kv.me)
		}

		if stuckWaiting || configStuck {
			// 查询最新配置
			latestConfig := kv.mck.Query(-1)
			kv.mu.Lock()
			if latestConfig.Num > currentConfig.Num {
				DPrintf("Group %d Server %d validateConfigState: found newer config %d, current is %d",
					kv.gid, kv.me, latestConfig.Num, currentConfig.Num)

				// 异步提交新配置
				go func(cfg shardctrler.Config) {
					op := Op{
						Type:   OpConfig,
						Config: cfg,
					}
					kv.rf.Start(op)
				}(latestConfig)
			} else if waitingCount > 0 {
				// 如果没有更新的配置但仍有等待的分片，可能需要重新尝试拉取
				DPrintf("Group %d Server %d validateConfigState: 可能需要重新尝试拉取分片，手动触发一次pullShardsLoop",
					kv.gid, kv.me)
				// 这里不做任何操作，让pullShardsLoop自己处理
			}
			kv.mu.Unlock()
		}

		time.Sleep(checkInterval)
	}
}

// 检查分片是否可用于服务请求
func (kv *ShardKV) isShardAvailable(shard int) bool {
	// 检查分片是否属于本组
	if kv.config.Shards[shard] != kv.gid {
		return false
	}

	// 检查分片状态
	if kv.shardStates[shard] == ShardMigrating || kv.shardStates[shard] == ShardWaiting {
		return false
	}

	// 检查分片是否正在等待迁移
	if _, waiting := kv.waitingShards[shard]; waiting {
		return false
	}

	return true
}

type SnapshotState struct {
	Data               map[int]map[string]string
	ClientSeq          map[int64]int
	Config             shardctrler.Config
	PrevConfig         shardctrler.Config
	WaitingShards      map[int]bool
	OutShards          map[int]map[int]map[string]string
	LastIncludeIndex   int
	InitialClients     map[int64]bool
	ShardStates        map[int]int
	LastConfigTimeUnix int64
	ReplayComplete     bool
}
