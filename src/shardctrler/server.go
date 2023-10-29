package shardctrler

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	replyChMap   map[int]chan ApplyNotifyMsg
	clientMaxSeq map[int64]CommandContext
	configs      []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operation string
	ClientId  int64
	CommandId int
	Servers   map[int][]string // new GID -> servers mappings
	GIDs      []int
	Shard     int
	GID       int
	Num       int
}

type ApplyNotifyMsg struct {
	Err    Err
	Term   int
	Config Config
}

type CommandContext struct {
	CommandId int
	Msg       ApplyNotifyMsg
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	reply.WrongLeader = false
	lastCommandContext, ok := sc.clientMaxSeq[args.ClientId]
	if ok {
		if args.CommandId <= lastCommandContext.CommandId {
			reply.Err = lastCommandContext.Msg.Err
			sc.mu.Unlock()
			return
		}
	}
	command := Op{Operation: args.Operation, ClientId: args.ClientId, CommandId: args.CommandId, Servers: args.Servers}
	DPrintf("sc[%d],join servers[%v]", sc.me, args.Servers)
	index, term, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	reply.WrongLeader = false
	lastCommandContext, ok := sc.clientMaxSeq[args.ClientId]
	if ok {
		if args.CommandId <= lastCommandContext.CommandId {
			reply.Err = lastCommandContext.Msg.Err
			sc.mu.Unlock()
			return
		}
	}
	command := Op{Operation: args.Operation, ClientId: args.ClientId, CommandId: args.CommandId, GIDs: args.GIDs}
	DPrintf("sc[%d],leave GIDs[%v]", sc.me, args.GIDs)
	index, term, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	reply.WrongLeader = false
	lastCommandContext, ok := sc.clientMaxSeq[args.ClientId]
	if ok {
		if args.CommandId <= lastCommandContext.CommandId {
			reply.Err = lastCommandContext.Msg.Err
			sc.mu.Unlock()
			return
		}
	}
	command := Op{Operation: args.Operation, ClientId: args.ClientId, CommandId: args.CommandId, Shard: args.Shard, GID: args.GID}
	index, term, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	reply.WrongLeader = false
	lastCommandContext, ok := sc.clientMaxSeq[args.ClientId]
	if ok {
		if args.CommandId <= lastCommandContext.CommandId {
			reply.Err = lastCommandContext.Msg.Err
			reply.Config = lastCommandContext.Msg.Config
			sc.mu.Unlock()
			return
		}
	}
	command := Op{Operation: args.Operation, ClientId: args.ClientId, CommandId: args.CommandId, Num: args.Num}
	// raft.DPrintf("sc[%d],query num[%d]", sc.me, args.Num)
	index, term, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
			reply.Config = replyMsg.Config
			// DPrintf("query %d\n", len(reply.Config.Groups))
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				sc.applyCommand(applyMsg)
			}
		}

	}

}
func GrouptoShards(config Config) (g2s map[int][]int) {
	g2s = make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for k, v := range config.Shards {
		g2s[v] = append(g2s[v], k)
	}
	DPrintf("shards[%v],g2s:[%v]", config.Shards, g2s)
	return
}
func getminShardsGid(g2s map[int][]int) int {
	gids := make([]int, len(g2s))
	i := 0
	for k := range g2s {
		gids[i] = k
		i++
	}
	sort.Ints(gids)
	index := -1
	min := NShards + 1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < min {
			min = len(g2s[gid])
			index = gid
		}
	}
	return index
}
func getmaxShardsGid(g2s map[int][]int) int {
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}
	gids := make([]int, len(g2s))
	i := 0
	for k := range g2s {
		gids[i] = k
		i++
	}
	sort.Ints(gids)
	index := -1
	max := -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) > max {
			max = len(g2s[gid])
			index = gid
		}
	}
	return index
}
func distributeShards(config Config) (newShards [NShards]int) {
	g2s := GrouptoShards(config)
	// DPrintf("before g2s:[%v],shards:[%v]", g2s, newShards)
	for {
		minGid := getminShardsGid(g2s)
		maxGid := getmaxShardsGid(g2s)
		// DPrintf("minGid:%d,maxGid:%d,g2s:[%v]", minGid, maxGid, g2s)
		if maxGid != 0 && len(g2s[maxGid])-len(g2s[minGid]) <= 1 {
			break
		}
		g2s[minGid] = append(g2s[minGid], g2s[maxGid][0])
		g2s[maxGid] = g2s[maxGid][1:]
		// DPrintf("g2s[minGid]:[%v]", g2s[minGid])
	}
	// DPrintf("after g2s:[%v],shards:[%v]", g2s, newShards)
	for gid, shards := range g2s {
		for _, shardId := range shards {
			newShards[shardId] = gid
		}
	}
	return
}
func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}
func (sc *ShardCtrler) applyCommand(applyMsg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// commandIndex := applyMsg.CommandIndex
	command := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex
	lastCommandContext, ok := sc.clientMaxSeq[command.ClientId]
	if ok {
		if command.CommandId <= lastCommandContext.CommandId {
			return
		}
	}
	if command.Operation == "Join" {
		newConfig := Config{}
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig.Num = lastConfig.Num + 1
		newConfig.Groups = deepCopy(lastConfig.Groups)
		for gid, servers := range command.Servers {
			_, ok := newConfig.Groups[gid]
			if !ok {
				newServers := make([]string, len(servers))
				copy(newServers, servers)
				newConfig.Groups[gid] = newServers
			}
		}
		newConfig.Shards = distributeShards(newConfig)
		DPrintf("distri config:[%v]", newConfig)
		sc.configs = append(sc.configs, newConfig)
		replyMsg := ApplyNotifyMsg{Term: applyMsg.CommandTerm, Err: OK}
		sc.clientMaxSeq[command.ClientId] = CommandContext{CommandId: command.CommandId, Msg: replyMsg}
		channel, ok := sc.replyChMap[index]
		if ok {
			channel <- replyMsg
		}
	} else if command.Operation == "Leave" {
		newConfig := Config{}
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig.Num = lastConfig.Num + 1
		newConfig.Groups = deepCopy(lastConfig.Groups)
		for _, gId := range command.GIDs {
			if _, ok := newConfig.Groups[gId]; ok {
				delete(newConfig.Groups, gId)
			}
		}
		DPrintf("delete GIDs:[%v]", command.GIDs)
		if len(newConfig.Groups) > 0 {
			newConfig.Shards = distributeShards(newConfig)
		}
		sc.configs = append(sc.configs, newConfig)
		replyMsg := ApplyNotifyMsg{Term: applyMsg.CommandTerm, Err: OK}
		sc.clientMaxSeq[command.ClientId] = CommandContext{CommandId: command.CommandId, Msg: replyMsg}
		channel, ok := sc.replyChMap[index]
		if ok {
			channel <- replyMsg
		}
	} else if command.Operation == "Move" {
		newConfig := Config{}
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig.Num = lastConfig.Num + 1
		newConfig.Groups = deepCopy(lastConfig.Groups)
		copy(newConfig.Shards[:], lastConfig.Shards[:])
		DPrintf("Move config.Shards:[%v]", newConfig.Shards)
		newConfig.Shards[command.Shard] = command.GID
		DPrintf("Move newConfig.Shards:[%v]", newConfig.Shards)
		sc.configs = append(sc.configs, newConfig)
		replyMsg := ApplyNotifyMsg{Term: applyMsg.CommandTerm, Err: OK}
		sc.clientMaxSeq[command.ClientId] = CommandContext{CommandId: command.CommandId, Msg: replyMsg}
		channel, ok := sc.replyChMap[index]
		if ok {
			channel <- replyMsg
		}
	} else if command.Operation == "Query" {
		config := Config{}
		if command.Num == -1 || command.Num > len(sc.configs)-1 {
			config = sc.configs[len(sc.configs)-1]
		} else {
			config = sc.configs[command.Num]
		}
		replyMsg := ApplyNotifyMsg{Term: applyMsg.CommandTerm, Err: OK, Config: config}
		sc.clientMaxSeq[command.ClientId] = CommandContext{CommandId: command.CommandId, Msg: replyMsg}
		channel, ok := sc.replyChMap[index]
		if ok {
			channel <- replyMsg
		}
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientMaxSeq = make(map[int64]CommandContext)
	sc.replyChMap = make(map[int]chan ApplyNotifyMsg)
	go sc.applier()
	return sc
}
