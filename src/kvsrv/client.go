package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server        *labrpc.ClientEnd
	clientId      int64
	nextRequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
// +
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: atomic.AddInt64(&ck.nextRequestId, 1),
	}
	reply := &GetReply{}
	for {
		if ok := ck.server.Call("KVServer.Get", args, reply); ok {
			if reply.Err == "ErrNoKey" {
				return ""
			}
			return reply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Version:   0,
		ClientId:  ck.clientId,
		RequestId: atomic.AddInt64(&ck.nextRequestId, 1),
	}
	reply := &PutAppendReply{}
	for {
		if ok := ck.server.Call("KVServer."+op, args, reply); ok {
			if reply.Err == "" {
				if op == "Append" {
					return reply.Value
				}
				return ""
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
