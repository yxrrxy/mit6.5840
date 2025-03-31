package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change its mind if a group becomes dead.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrOutDated       = "ErrOutDated"
	ErrTimeout        = "ErrTimeout" // 超时错误
	ErrWrongOperation = "ErrWrongOperation"
)

type Err string

// 操作类型
type OpType string

const (
	Get    OpType = "Get"
	Put    OpType = "Put"
	Append OpType = "Append"
)

// 客户端命令参数
type CommandArgs struct {
	Key       string // 操作的键
	Value     string // 操作的值（用于Put和Append）
	Op        OpType // 操作类型
	ClientId  int64  // 客户端ID
	CommandId int64  // 命令ID，用于幂等性检测
}

// 命令执行结果
type CommandReply struct {
	Err   Err    // 错误信息
	Value string // 返回的值（用于Get）
}

// 用于存储客户端操作的上下文
type OperationContext struct {
	MaxAppliedCommandId int64         // 最大应用的命令ID
	LastReply           *CommandReply // 上一个回复
}

// 分片操作参数
type ShardOperationArgs struct {
	ConfigNum int   // 配置号
	ShardIDs  []int // 分片IDs
}

// 分片操作回复
type ShardOperationReply struct {
	Err            Err                        // 错误信息
	ConfigNum      int                        // 配置号
	Shards         map[int]map[string]string  // 分片数据
	LastOperations map[int64]OperationContext // 最后操作上下文
}

// 客户端执行Get操作的参数
type GetArgs struct {
	Key      string // 要获取的键
	ClientId int64  // 客户端ID
	SeqNum   int    // 序列号，用于幂等性检测
}

// Get操作的回复
type GetReply struct {
	Err   Err    // 错误信息
	Value string // 获取的值
}

// 客户端执行Put/Append操作的参数
type PutAppendArgs struct {
	Key      string // 要操作的键
	Value    string // 操作的值
	Op       string // "Put" 或 "Append"
	ClientId int64  // 客户端ID
	SeqNum   int    // 序列号，用于幂等性检测
}

// Put/Append操作的回复
type PutAppendReply struct {
	Err Err // 错误信息
}

// 分片迁移相关的 RPC 参数和回复
type GetShardArgs struct {
	ConfigNum      int   // 请求配置号
	Shard          int   // 请求的分片
	RequestHistory []int // 配置历史记录，追踪分片所有权变更
}

type GetShardReply struct {
	Err           Err
	ShardData     map[string]string
	ClientSeq     map[int64]int
	ConfigNum     int            // 返回的配置号
	CurrentOwner  int            // 当前拥有此分片的组ID
	ConfigHistory []int          // 已知的配置历史
	DataSizes     map[string]int // 记录每个键对应值的大小
}

type CleanShardArgs struct {
	ConfigNum int
	Shard     int
}

type CleanShardReply struct {
	Err Err
}

// 配置通知相关的RPC参数和回复
type NotifyConfigArgs struct {
	ConfigNum int
	Config    shardctrler.Config
}

type NotifyConfigReply struct {
	Err Err
}
