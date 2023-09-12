package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	CommandId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	replyChMap        map[int]chan ApplyNotifyMsg
	clientMaxSeq      map[int64]CommandContext
	DB                map[string]string
	lastIncludedIndex int
}

type ApplyNotifyMsg struct {
	Err   Err
	Term  int
	Value string
}

type CommandContext struct {
	CommandId int
	Msg       ApplyNotifyMsg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	lastCommandContext, ok := kv.clientMaxSeq[args.ClientId]
	if ok {
		if args.CommandId <= lastCommandContext.CommandId {
			reply.Err = lastCommandContext.Msg.Err
			reply.Value = lastCommandContext.Msg.Value
			kv.mu.Unlock()
			return
		}
	}
	command := Op{Operation: args.Operation, Key: args.Key, ClientId: args.ClientId, CommandId: args.CommandId}
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	lastCommandContext, ok := kv.clientMaxSeq[args.ClientId]
	if ok {
		if args.CommandId <= lastCommandContext.CommandId {
			reply.Err = lastCommandContext.Msg.Err
			kv.mu.Unlock()
			return
		}
	}
	command := Op{Operation: args.Operation, Key: args.Key, Value: args.Value, ClientId: args.ClientId, CommandId: args.CommandId}
	//DPrintf("kvserver[%d]:开始raft同步key为%v,value为%v\n", kv.me, command.Key, command.Value)
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		if term == replyMsg.Term {
			//DPrintf("PutAppend term为%d,回复的term为%d,回复的value为%v", term, replyMsg.Term, replyMsg.Value)
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var DB map[string]string
	var clientMaxSeq map[int64]CommandContext
	if d.Decode(&DB) != nil || d.Decode(&clientMaxSeq) != nil {
		DPrintf("安装日志失败")
	} else {
		kv.DB = DB
		kv.clientMaxSeq = clientMaxSeq
	}
}
func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.applyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				if applyMsg.SnapshotIndex < kv.lastIncludedIndex {
					return
				} else {
					kv.lastIncludedIndex = applyMsg.SnapshotIndex
					kv.readSnapShot(applyMsg.Snapshot)
				}
			}
		}

	}

}

func (kv *KVServer) applyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commandIndex := applyMsg.CommandIndex
	command := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex
	lastCommandContext, ok := kv.clientMaxSeq[command.ClientId]
	replyMsg := ApplyNotifyMsg{}
	if ok {
		if command.CommandId <= lastCommandContext.CommandId {
			return
		}
	}
	if command.Operation == "Get" {
		value, ok := kv.DB[command.Key]
		if ok {
			replyMsg = ApplyNotifyMsg{Value: value, Term: applyMsg.CommandTerm, Err: OK}
			//DPrintf("kvserver[%d]:完成Get,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
		} else {
			replyMsg = ApplyNotifyMsg{Value: value, Term: applyMsg.CommandTerm, Err: ErrNoKey}
		}
	} else if command.Operation == "Put" {
		kv.DB[command.Key] = command.Value
		replyMsg = ApplyNotifyMsg{Value: command.Value, Term: applyMsg.CommandTerm, Err: OK}
		//DPrintf("kvserver[%d]:完成Put,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
	} else if command.Operation == "Append" {
		kv.DB[command.Key] += command.Value
		replyMsg = ApplyNotifyMsg{Value: kv.DB[command.Key], Term: applyMsg.CommandTerm, Err: OK}
		//DPrintf("kvserver[%d]:完成Append,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
	}
	channel, ok := kv.replyChMap[index]
	if ok {
		channel <- replyMsg
	}
	kv.clientMaxSeq[command.ClientId] = CommandContext{CommandId: command.CommandId, Msg: replyMsg}
	kv.lastIncludedIndex = index
	if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.DB)
		e.Encode(kv.clientMaxSeq)
		go kv.rf.Snapshot(commandIndex, w.Bytes())
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientMaxSeq = make(map[int64]CommandContext)
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.DB = make(map[string]string)

	kv.readSnapShot(kv.rf.GetSnapShot())
	// You may need initialization code here.
	go kv.applier()
	return kv
}
