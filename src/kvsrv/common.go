package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Version   int64
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	Err     string
	Value   string
	Version int64
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err     string
	Value   string
	Version int64
}
