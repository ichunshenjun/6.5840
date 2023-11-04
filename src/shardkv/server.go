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

// const Debug = true

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
	lastSnapshot      int
	shards            map[int]*Shard
	lastConfig        shardctrler.Config
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

type PullShardArgs struct {
	ConfigNum int
	ShardIds  []int
}
type PullShardReply struct {
	Err       Err
	ConfigNum int
	Shards    map[int]*Shard
}

type DeleteShardArgs PullShardArgs
type DeleteShardReply PullShardReply

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

/******tools**********/
func NewShard(status ShardStatus) *Shard {
	return &Shard{make(map[string]string), status}
}

func (kv *ShardKV) getAllShards(nextConfig *shardctrler.Config) []int {
	var shardIds []int
	DPrintf("G%+v {S%+v},getAllShards:nextConfig:%v,kv.gid:%d", kv.gid, kv.me, nextConfig, kv.gid)
	for shardId, gid := range nextConfig.Shards {
		if gid == kv.gid {
			shardIds = append(shardIds, shardId)
		}
	}
	return shardIds
}

func (kv *ShardKV) getShardIdsByStatus(status ShardStatus) map[int][]int {
	g2s := make(map[int][]int)
	for shardId, _ := range kv.shards {
		if kv.shards[shardId].Status == status {
			gid := kv.lastConfig.Shards[shardId]
			if _, ok := g2s[gid]; !ok {
				arr := [1]int{shardId}
				g2s[gid] = arr[:]
			} else {
				g2s[gid] = append(g2s[gid], shardId)
			}
		}
	}
	DPrintf("G%+v {S%+v},getShardIdsByStatus:kv.lastConfig.Shards:%v,kv.currCOnfig.Shards:%v,g2s:%v", kv.gid, kv.me, kv.lastConfig.Shards, kv.currConfig.Shards, g2s)
	return g2s
}

func (shard *Shard) deepCopy() *Shard {
	newShard := NewShard(Serving)
	for k, v := range shard.KV {
		newShard.KV[k] = v
	}
	return newShard
}

/********************
*********************/
func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currConfig.Shards[shardId] == kv.gid && (kv.shards[shardId].Status == Serving || kv.shards[shardId].Status == GCing)
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.canServe(shardId) {
		DPrintf("G%+v {S%+v},分片%+v所属的组:[%v] Get: ", kv.gid, kv.me, shardId, kv.currConfig.Shards[shardId])
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
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
	// DPrintf("kvserver[%d]:开始raft同步key为%v,value为%v\n", kv.me, command.Data.(Op).Key, command.Data.(Op).Value)
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
	shardId := key2shard(args.Key)
	if !kv.canServe(shardId) {
		DPrintf("kvserver[%v]:PutAppend分片不能服务", kv.me)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	lastCommandContext, ok := kv.clientMaxSeq[args.ClientId]
	if ok {
		if args.CommandId <= lastCommandContext.CommandId {
			reply.Err = lastCommandContext.Msg.Err
			kv.mu.Unlock()
			return
		}
	}
	command := Command{Operation, Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, CommandId: args.CommandId}}
	// DPrintf("kvserver[%d]:开始raft同步key为%v,value为%v\n", kv.me, command.Key, command.Value)
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
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards map[int]*Shard
	var lastConfig, currConfig shardctrler.Config
	var clientMaxSeq map[int64]CommandContext
	if d.Decode(&shards) != nil || d.Decode(lastConfig) != nil || d.Decode(currConfig) != nil || d.Decode(&clientMaxSeq) != nil {
		DPrintf("安装日志失败")
	} else {
		kv.shards = shards
		kv.lastConfig = lastConfig
		kv.currConfig = currConfig
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

func (kv *ShardKV) applyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// commandIndex := applyMsg.CommandIndex
	command := applyMsg.Command.(Command)
	index := applyMsg.CommandIndex
	replyMsg := ApplyNotifyMsg{}
	if applyMsg.CommandIndex <= kv.lastIncludedIndex {
		return
	}
	switch command.CmdType {
	case DeleteShards:
		deleteShardsInfo := command.Data.(DeleteShardArgs)
		replyMsg = *kv.applyDeleteShards(&deleteShardsInfo)
	case InsertShards:
		insertShardsInfo := command.Data.(PullShardReply)
		replyMsg = *kv.applyInsertShards(&insertShardsInfo)
	case Configuration:
		nextConfig := command.Data.(shardctrler.Config)
		DPrintf("G%+v {S%+v},applyCommand:nextConfig:%v", kv.gid, kv.me, nextConfig)
		replyMsg = *kv.applyConfiguration(&nextConfig)
	case Operation:
		op := command.Data.(Op)
		shardId := key2shard(op.Key)
		if !kv.canServe(shardId) {
			DPrintf("kv[%v]:Get无法服务了!!!!!!!!!!!!!!!", kv.me)
			replyMsg = ApplyNotifyMsg{Err: ErrWrongGroup, Value: ""}
		} else {
			if op.Operation == "Get" {
				value, ok := kv.shards[shardId].KV[op.Key]
				if ok {
					replyMsg = ApplyNotifyMsg{Value: value, Term: applyMsg.CommandTerm, Err: OK}
					//DPrintf("kvserver[%d]:完成Get,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
				} else {
					replyMsg = ApplyNotifyMsg{Value: value, Term: applyMsg.CommandTerm, Err: ErrNoKey}
				}
			} else if op.Operation == "Put" {
				kv.shards[shardId].KV[op.Key] = op.Value
				replyMsg = ApplyNotifyMsg{Value: op.Value, Term: applyMsg.CommandTerm, Err: OK}
				//DPrintf("kvserver[%d]:完成Put,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
			} else if op.Operation == "Append" {
				kv.shards[shardId].KV[op.Key] += op.Value
				replyMsg = ApplyNotifyMsg{Value: kv.DB[op.Key], Term: applyMsg.CommandTerm, Err: OK}
				//DPrintf("kvserver[%d]:完成Append,key为%v,value为%v,commandIndex为%v", kv.me, command.Key, command.Value, commandIndex)
			}
			kv.clientMaxSeq[op.ClientId] = CommandContext{CommandId: op.CommandId, Msg: replyMsg}
		}
	}
	channel, ok := kv.replyChMap[index]
	if ok {
		channel <- replyMsg
	}
	kv.lastIncludedIndex = index
	// if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate && commandIndex-kv.rf.GetSnapShotIndex() >= 20 {
	// 	w := new(bytes.Buffer)
	// 	e := labgob.NewEncoder(w)
	// 	e.Encode(kv.shards)
	// 	e.Encode(kv.lastConfig)
	// 	e.Encode(kv.currConfig)
	// 	e.Encode(kv.clientMaxSeq)
	// 	DPrintf("kuaizhao.............")
	// 	go kv.rf.Snapshot(commandIndex, w.Bytes())
	// }
}

/**********Configuration Update*************/
// 拉取新配置
func (kv *ShardKV) configurationAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.shards {
		if shard.Status != Serving {
			// DPrintf("G%+v {S%+v},shard.Status:%v", kv.gid, kv.me, shard.Status)
			canPerformNextConfig = false
			break
		}
	}
	currConfigNum := kv.currConfig.Num
	kv.mu.Unlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currConfigNum + 1)
		DPrintf("G%+v {S%+v},configurationAction:nextConfig:[%v],currConfig:[%v],kv.shards:[%v],kv.gid:%d", kv.gid, kv.me, nextConfig, kv.currConfig, kv.shards, kv.gid)
		if nextConfig.Num == currConfigNum+1 {
			command := Command{Configuration, nextConfig}
			kv.Execute(command, &ApplyNotifyMsg{})
		}
	}
}

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *ApplyNotifyMsg {
	if nextConfig.Num == kv.currConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = shardctrler.Config{Num: kv.currConfig.Num, Shards: kv.currConfig.Shards, Groups: shardctrler.DeepCopy(kv.currConfig.Groups)}
		kv.currConfig = shardctrler.Config{Num: nextConfig.Num, Shards: nextConfig.Shards, Groups: shardctrler.DeepCopy(nextConfig.Groups)}
		return &ApplyNotifyMsg{Err: OK, Value: ""}
	}
	return &ApplyNotifyMsg{Err: ErrOutDated, Value: ""}
}

// 根据新配置修改分片状态
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	// defer DPrintf("kv[%d]:更新分片状态，kv.shards:%v", kv.me, kv.shards)
	if nextConfig.Num == 1 {
		shardIds := kv.getAllShards(nextConfig)
		for _, shardId := range shardIds {
			kv.shards[shardId] = NewShard(Serving)
		}
		DPrintf("G%+v {S%+v},updateShardStatus,shardIds:%v,kv.shards:%v", kv.gid, kv.me, shardIds, kv.shards)
		return
	}
	nextShardIds := kv.getAllShards(nextConfig)
	currShardIds := kv.getAllShards(&kv.currConfig)
	for _, currShardId := range currShardIds {
		if nextConfig.Shards[currShardId] != kv.gid {
			kv.shards[currShardId].Status = BePulling
		}
	}
	for _, nextShardId := range nextShardIds {
		if kv.currConfig.Shards[nextShardId] != kv.gid {
			kv.shards[nextShardId] = NewShard(Pulling)
		}
	}
	DPrintf("G%+v {S%+v},updateShardStatus,nextShardIds:%v,currShardIds:%v,kv.shards:%v", kv.gid, kv.me, nextShardIds, currShardIds, kv.shards)
}

/**********Shard Migration*************/
func (kv *ShardKV) migrationAction() {
	kv.mu.Lock()
	g2s := kv.getShardIdsByStatus(Pulling)
	DPrintf("G%+v {S%+v},migrationAction:g2s:%v", kv.gid, kv.me, g2s)
	var wg sync.WaitGroup
	for gid, shardIds := range g2s {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := PullShardArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}
			for _, server := range servers {
				var reply PullShardReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.PullShardsData", &args, &reply) && reply.Err == OK {
					command := Command{InsertShards, reply}
					kv.Execute(command, &ApplyNotifyMsg{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currConfig.Num, shardIds)
	}
	kv.mu.Unlock()
	wg.Wait()
}
func (kv *ShardKV) PullShardsData(args *PullShardArgs, reply *PullShardReply) {
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.currConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		kv.configurationAction()
		return
	}
	reply.Shards = make(map[int]*Shard)
	for _, shardId := range args.ShardIds {
		reply.Shards[shardId] = kv.shards[shardId].deepCopy()
	}
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
	kv.mu.Unlock()
	DPrintf("G%+v {S%+v} PullShardsData: args: %+v reply: %+v", kv.gid, kv.me, args, reply)
}
func (kv *ShardKV) applyInsertShards(insertShardsInfo *PullShardReply) *ApplyNotifyMsg {
	if insertShardsInfo.ConfigNum == kv.currConfig.Num {
		for shardId, shardData := range insertShardsInfo.Shards {
			if kv.shards[shardId].Status == Pulling {
				kv.shards[shardId] = shardData.deepCopy()
				kv.shards[shardId].Status = GCing
				// kv.shards[shardId].Status = Serving
			} else {
				break
			}
		}
		return &ApplyNotifyMsg{Err: OK, Value: ""}
	}
	return &ApplyNotifyMsg{Err: ErrOutDated, Value: ""}
}

/**********Shard Garbage Collection*************/
func (kv *ShardKV) gcAction() {
	kv.mu.Lock()
	g2s := kv.getShardIdsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIds := range g2s {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := DeleteShardArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}
			for _, server := range servers {
				var reply DeleteShardReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.DeleteShardsData", &args, &reply) && reply.Err == OK {
					command := Command{DeleteShards, args}
					kv.Execute(command, &ApplyNotifyMsg{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currConfig.Num, shardIds)
	}
	kv.mu.Unlock()
	wg.Wait()
}
func (kv *ShardKV) DeleteShardsData(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.currConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	var replyMsg ApplyNotifyMsg
	command := Command{DeleteShards, *args}
	kv.Execute(command, &replyMsg)
	reply.Err = replyMsg.Err
}
func (kv *ShardKV) applyDeleteShards(deleteShardsInfo *DeleteShardArgs) *ApplyNotifyMsg {
	if deleteShardsInfo.ConfigNum == kv.currConfig.Num {
		for _, shardId := range deleteShardsInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulling {
				kv.shards[shardId] = NewShard(Serving)
			} else {
				break
			}
		}
		return &ApplyNotifyMsg{Err: OK, Value: ""}
	}
	return &ApplyNotifyMsg{Err: OK, Value: ""}
}

/**********SnapShot*************/
func (kv *ShardKV) snapshoter() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.isNeedSnapShot() {
			kv.takeSnapshot(kv.lastIncludedIndex)
			kv.lastSnapshot = kv.lastIncludedIndex
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}
func (kv *ShardKV) isNeedSnapShot() bool {
	for _, shard := range kv.shards {
		if shard.Status == BePulling {
			return false
		}
	}
	if kv.maxraftstate != -1 {
		threshold := int(0.8 * float32(kv.maxraftstate))
		if kv.rf.GetStateSize() > threshold || kv.lastIncludedIndex > kv.lastSnapshot+3 {
			return true
		}
	}
	return false
}
func (kv *ShardKV) takeSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currConfig)
	e.Encode(kv.clientMaxSeq)
	DPrintf("kuaizhao.............")
	go kv.rf.Snapshot(commandIndex, w.Bytes())
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
	labgob.Register(PullShardArgs{})
	labgob.Register(PullShardReply{})
	labgob.Register(DeleteShardArgs{})
	labgob.Register(DeleteShardReply{})
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
	kv.lastIncludedIndex = 0
	kv.lastSnapshot = 0
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg, 5)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapShot(kv.rf.GetSnapShot())
	// You may need initialization code here.
	go kv.applier()
	go kv.snapshoter()
	go kv.ticker(kv.configurationAction, 50)
	go kv.ticker(kv.migrationAction, 50)
	go kv.ticker(kv.gcAction, 50)
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
