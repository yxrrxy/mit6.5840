package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

// 添加错误常量
const (
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	store   map[string]KeyValue
	applied map[int64]LastOp // clientId -> LastOp
}

type KeyValue struct {
	Value   string
	Version int64
}

type LastOp struct {
	RequestId int64
	Response  string
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]KeyValue)
	kv.applied = make(map[int64]LastOp)
	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查重复请求
	//if lastOp, ok := kv.applied[args.ClientId]; ok {
	//	if args.RequestId <= lastOp.RequestId {
	//		reply.Value = lastOp.Response
	//		return
	//	}
	//}

	if val, ok := kv.store[args.Key]; ok {
		reply.Value = val.Value
		reply.Version = val.Version
	} else {
		reply.Err = ErrNoKey
	}

	// 记录这次请求
	//kv.applied[args.ClientId] = LastOp{
	//	RequestId: args.RequestId,
	//	Response:  reply.Value,
	//}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查重复请求
	//if lastOp, ok := kv.applied[args.ClientId]; ok {
	//	if args.RequestId <= lastOp.RequestId {
	//		reply.Value = lastOp.Response
	//		return
	//	}
	//}

	if err := kv.checkVersion(args.Key, args.Version); err != "" {
		reply.Err = err
		return
	}

	kv.store[args.Key] = KeyValue{
		Value:   args.Value,
		Version: args.Version + 1,
	}
	reply.Version = args.Version + 1

	// 记录这次请求
	//kv.applied[args.ClientId] = LastOp{
	//	RequestId: args.RequestId,
	//	Response:  args.Value,
	//}
}

// 添加辅助方法
func (kv *KVServer) checkVersion(key string, version int64) string {
	if version > 0 {
		if val, ok := kv.store[key]; !ok {
			return ErrNoKey
		} else if val.Version != version {
			return ErrVersion
		}
	}
	return ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查重复请求
	if lastOp, ok := kv.applied[args.ClientId]; ok {
		if args.RequestId <= lastOp.RequestId {
			reply.Value = lastOp.Response
			return
		}
	}

	// 获取当前值
	oldValue := ""
	if val, ok := kv.store[args.Key]; ok {
		oldValue = val.Value
	}

	// 执行追加操作
	newValue := oldValue + args.Value
	newVersion := int64(1)
	if val, ok := kv.store[args.Key]; ok {
		newVersion = val.Version + 1
	}

	// 更新存储
	kv.store[args.Key] = KeyValue{
		Value:   newValue,
		Version: newVersion,
	}

	// 记录这次请求
	kv.applied[args.ClientId] = LastOp{
		RequestId: args.RequestId,
		Response:  oldValue,
	}

	// 设置响应
	reply.Value = oldValue
	reply.Version = newVersion
}
