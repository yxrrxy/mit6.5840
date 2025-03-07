package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int
	clientId  int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientId,
		RequestID: atomic.AddInt64(&ck.requestId, 1),
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			serverId := (ck.leaderId + i) % len(ck.servers)
			reply := GetReply{}

			if ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply); ok {
				if reply.Err != ErrWrongLeader {
					ck.leaderId = serverId
					if reply.Err == ErrNoKey {
						return ""
					}
					return reply.Value
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientId,
		RequestID: atomic.AddInt64(&ck.requestId, 1),
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			serverId := (ck.leaderId + i) % len(ck.servers)
			reply := PutAppendReply{}

			if ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply); ok {
				if reply.Err != ErrWrongLeader {
					ck.leaderId = serverId
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
