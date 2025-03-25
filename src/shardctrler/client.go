package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	seqNum   int
	leader   int // 上次已知的领导者
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
	// Your code here.
	ck.clientId = nrand()
	ck.seqNum = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.seqNum++ //单线程就直接++

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(ck.leader+i)%len(ck.servers)]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leader = (ck.leader + i) % len(ck.servers) // 记住成功的领导者
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.seqNum++

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(ck.leader+i)%len(ck.servers)]
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leader = (ck.leader + i) % len(ck.servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.seqNum++

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(ck.leader+i)%len(ck.servers)]
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leader = (ck.leader + i) % len(ck.servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.seqNum++

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(ck.leader+i)%len(ck.servers)]
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leader = (ck.leader + i) % len(ck.servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
