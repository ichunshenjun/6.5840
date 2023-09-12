package kvraft

import (
	"6.5840/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId             int
	clientId             int64
	lastAppliedCommandId int
	mu                   sync.Mutex
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.lastAppliedCommandId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	commandId := ck.lastAppliedCommandId + 1
	serverId := ck.leaderId
	serverNum := len(ck.servers)
	for ; ; serverId = (serverId + 1) % serverNum {
		args := &GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			CommandId: commandId,
			Operation: "Get",
		}
		reply := &GetReply{}
		DPrintf("client[%d]:发送Get RPC[%d]到server[%d]\n", ck.clientId, commandId, serverId)
		ok := ck.servers[serverId].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			DPrintf("client[%d]:发送Get RPC失败因为%v", ck.clientId, reply.Err)
			continue
		}
		ck.leaderId = serverId
		ck.lastAppliedCommandId = commandId
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
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
	// You will have to modify this function.
	commandId := ck.lastAppliedCommandId + 1
	serverId := ck.leaderId
	serverNum := len(ck.servers)
	//DPrintf("op为%v,value为%v", op, value)
	for ; ; serverId = (serverId + 1) % serverNum {
		args := &PutAppendArgs{key, value, op, ck.clientId, commandId}
		reply := &PutAppendReply{}
		//DPrintf("client[%d]:发送PutAppend RPC[%d]到server[%d],args为%v\n", ck.clientId, commandId, serverId, args)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			//DPrintf("client[%d]:发送PutAppend RPC失败因为%v", ck.clientId, reply.Err)
			continue
		}
		ck.leaderId = serverId
		ck.lastAppliedCommandId = commandId
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
