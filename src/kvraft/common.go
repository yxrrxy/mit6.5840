package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	ClientID  int64
	RequestID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientID  int64
	RequestID int64
}

type GetReply struct {
	Err   Err
	Value string
}
