package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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
	replyChMap        map[int]chan ApplyNotifyMsg
	clientMaxSeq      map[int64]CommandContext
	DB                map[string]string
	dead              int32
	lastIncludedIndex int
	shards            map[int]*Shard
	currConfig        shardctrler.Config
	sc                *shardctrler.Clerk //此sc非shardctrler的sc
}

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type Shard struct {
	KV     map[string]string
	Status ShardStatus
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

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type Command struct {
	CmdType CommandType
	Data    interface{}
}

func (kv *ShardKV) Execute(command Command, reply *ApplyNotifyMsg) {
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
	command := Command{Operation, Op{Operation: args.Op, Key: args.Key, ClientId: args.ClientId, CommandId: args.CommandId}}
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	command := Command{Operation, Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, CommandId: args.CommandId}}
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) readSnapShot(snapshot []byte) {
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
func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.applyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				if applyMsg.SnapshotIndex <= kv.lastIncludedIndex {
					return
				} else {
					kv.lastIncludedIndex = applyMsg.SnapshotIndex
					kv.readSnapShot(applyMsg.Snapshot)
				}
			}
		}

	}

}
func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *ApplyNotifyMsg {
	return &ApplyNotifyMsg{}
}
func (kv *ShardKV) applyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commandIndex := applyMsg.CommandIndex
	command := applyMsg.Command.(Command)
	index := applyMsg.CommandIndex
	replyMsg := ApplyNotifyMsg{}
	if applyMsg.CommandIndex <= kv.lastIncludedIndex {
		return
	}
	switch command.CmdType {
	case Configuration:
		nextConfig := command.Data.(shardctrler.Config)
		replyMsg = *kv.applyConfiguration(&nextConfig)
	case Operation:
		op := command.Data.(Op)
		if op.Operation == "Get" {
			value, ok := kv.DB[op.Key]
			if ok {
				replyMsg = ApplyNotifyMsg{Value: value, Term: applyMsg.CommandTerm, Err: OK}
				//DPrintf("kvserver[%d]:完成Get,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
			} else {
				replyMsg = ApplyNotifyMsg{Value: value, Term: applyMsg.CommandTerm, Err: ErrNoKey}
			}
		} else if op.Operation == "Put" {
			kv.DB[op.Key] = op.Value
			replyMsg = ApplyNotifyMsg{Value: op.Value, Term: applyMsg.CommandTerm, Err: OK}
			//DPrintf("kvserver[%d]:完成Put,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
		} else if op.Operation == "Append" {
			kv.DB[op.Key] += op.Value
			replyMsg = ApplyNotifyMsg{Value: kv.DB[op.Key], Term: applyMsg.CommandTerm, Err: OK}
			//DPrintf("kvserver[%d]:完成Append,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
		}
		channel, ok := kv.replyChMap[index]
		if ok {
			channel <- replyMsg
		}
		kv.clientMaxSeq[op.ClientId] = CommandContext{CommandId: op.CommandId, Msg: replyMsg}
		kv.lastIncludedIndex = index
		if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate && commandIndex-kv.rf.GetSnapShotIndex() >= 20 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.DB)
			e.Encode(kv.clientMaxSeq)
			go kv.rf.Snapshot(commandIndex, w.Bytes())
		}
	}
}

/**********Configuration Update*************/
func (kv *ShardKV) configurationAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.shards {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}
	currConfigNum := kv.currConfig.Num
	kv.mu.Unlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currConfigNum + 1)
		if nextConfig.Num == currConfigNum+1 {
			command := Command{Configuration, nextConfig}
			kv.Execute(command, &ApplyNotifyMsg{})
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.clientMaxSeq = make(map[int64]CommandContext)
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.DB = make(map[string]string)
	kv.shards = make(map[int]*Shard)
	kv.sc = shardctrler.MakeClerk(ctrlers)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapShot(kv.rf.GetSnapShot())
	// You may need initialization code here.
	go kv.applier()
	go kv.ticker(kv.configurationAction, 50)
	return kv
}

func (kv *ShardKV) ticker(fn func(), timeout int) {
	for !kv.killed() {
		if _, isleader := kv.rf.GetState(); isleader {
			fn()
		}
		time.Sleep(time.Duration(timeout) * time.Millisecond)
	}
}
