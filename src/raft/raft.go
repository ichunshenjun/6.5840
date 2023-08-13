package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LEADER    = 0
	FOLLOWER  = 1
	CANDIDATE = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role         int
	currentTerm  int
	votedFor     int
	log          []Log
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	electionTime time.Time
	electionNum  int
	applyCh      chan ApplyMsg
	applyCond    *sync.Cond

	lastIncludedIndex int
	lastIncludedTerm  int
}
type Log struct {
	Context     interface{}
	CurrentTerm int
	LogIndex    int
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.role == LEADER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("解码错误\n")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = make([]Log, len(log))
		copy(rf.log, log)
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	for cutIndex, log := range rf.log {
		if log.LogIndex == index {
			rf.lastIncludedIndex = index
			rf.lastIncludedTerm = log.CurrentTerm
			rf.log = rf.log[cutIndex+1:]
			rf.log = append([]Log{{-1, 0, 0}}, rf.log...)
			if index > rf.commitIndex {
				rf.commitIndex = index
			}
			if index > rf.lastApplied {
				rf.lastApplied = index
			}
			//DPrintf("我是第%d号，我给人砍了，最后一个log索引为%d,index为%d", rf.me, rf.log[len(rf.log)-1].LogIndex, index)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(rf.votedFor)
			e.Encode(rf.currentTerm)
			e.Encode(rf.log)
			e.Encode(rf.lastIncludedIndex)
			e.Encode(rf.lastIncludedTerm)
			raftstate := w.Bytes()
			rf.persister.Save(raftstate, snapshot)
		}
	}
	//DPrintf("发生甚么事了index为%d\n", index)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
	HeartBeat    bool
}
type AppendEntriesReply struct {
	Term    int
	XTerm   int
	XIndex  int
	XLen    int
	Success bool
}

func (rf *Raft) findLastLogInTerm(XTerm int) int {
	for i := rf.getLastLogIndex(); i > 0; i-- {
		term := rf.getLogTerm(i)
		if term == XTerm {
			return i
		} else if term < XTerm {
			break
		}
	}
	return -1
}
func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}
func (rf *Raft) getLogIndex(index int) int {
	if index > rf.lastIncludedIndex {
		return rf.log[index-rf.lastIncludedIndex].LogIndex
	}
	return rf.lastIncludedIndex
}
func (rf *Raft) getLogTerm(index int) int {
	if index > rf.lastIncludedIndex {
		return rf.log[index-rf.lastIncludedIndex].CurrentTerm
	}
	return rf.lastIncludedTerm
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//if rf.role == LEADER {
		//	DPrintf("发生甚么事了\n")
		//}
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.persist()
	}
	//DPrintf("我是第%d号,entries长度为%d", rf.me, len(args.Entries))
	//if len(args.Entries) == 0 {
	//	return
	//}
	rf.role = FOLLOWER
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.XTerm = -1
		reply.XIndex = -1
		//reply.XLen = len(rf.log)
		reply.XLen = rf.getLastLogIndex() + 1
		reply.Success = false
		//DPrintf("wuhuwuhuwuhuwuhu,reply.XLen为%d,rf.last为%d,rf.getLastLogIndex()+1为%d", reply.XLen, rf.lastIncludedIndex, rf.getLastLogIndex()+1)
		return
	}
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		//DPrintf("我是第%d号,参数日志任期为%d,我对应的日志任期为%d,最后一条日志任期为%d", rf.me, args.PrevLogTerm, rf.log[args.PrevLogIndex].CurrentTerm, rf.log[len(rf.log)-1].CurrentTerm)
		reply.XTerm = rf.getLogTerm(args.PrevLogIndex)
		for index := args.PrevLogIndex; index > 0; index-- {
			//DPrintf("index为%d,rf.lastIncludedIndex为%d", index, rf.lastIncludedIndex)
			if rf.getLogTerm(index-1) != reply.XTerm {
				reply.XIndex = index
				break
			}
		}
		reply.XLen = len(rf.log)
		reply.Success = false
		return
	}
	reply.Success = true
	for idx, entry := range args.Entries {
		if entry.LogIndex <= rf.getLastLogIndex() && rf.getLogTerm(entry.LogIndex) != entry.CurrentTerm {
			//rf.log = rf.log[:(entry.LogIndex - rf.lastIncludedIndex)]
			rf.log = rf.log[:(rf.getLogIndex(entry.LogIndex) - rf.lastIncludedIndex)]

			rf.persist()
		}
		if entry.LogIndex > rf.getLastLogIndex() {
			rf.log = append(rf.log, args.Entries[idx:]...)
			rf.persist()
			break
		}
	}
	//rf.log = rf.log[:args.PrevLogIndex+1]
	//rf.log = append(rf.log, args.Entries...)
	//rf.persist()

	//for i := 0; i < len(args.Entries); i++ {
	//	DPrintf("我是第%d号,任期为%d,添加的日志为%d", rf.me, rf.currentTerm, args.Entries[i].LogIndex)
	//}
	//DPrintf("我是第%d号,任期为%d,添加log %d,内容为%v\n", rf.me, rf.currentTerm, rf.log[len(rf.log)-1].LogIndex, rf.log[len(rf.log)-1].Context)
	//DPrintf("我是第%d号，我已同步\n", rf.me)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Broadcast()
	}
	//if rf.commitIndex > rf.lastApplied {
	//	rf.lastApplied++
	//	msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Context, CommandIndex: rf.lastApplied}
	//	//DPrintf("提交%d的log\n", rf.commitIndex)
	//	rf.applyCh <- msg
	//}
	//if rf.role == LEADER {
	//	DPrintf("当前任期为%d,我是第 %d号，我再也不是leader了\n", rf.currentTerm, rf.me)
	//}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//rf.votedFor = -1
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		//return
	}
	//DPrintf("候选人最高任期为%d,本地任期为%d\n", args.LastLogTerm, rf.log[len(rf.log)-1].CurrentTerm)
	//DPrintf("候选人日志索引为%d,本地日志索引为%d\n", args.LastLogIndex, rf.log[len(rf.log)-1].LogIndex)
	//uptodate := args.LastLogTerm >rf.log[len(rf.log)-1].CurrentTerm || (args.LastLogTerm == rf.log[len(rf.log)-1].CurrentTerm && args.LastLogIndex >= len(rf.log)-1)
	uptodate := args.LastLogTerm > rf.getLogTerm(rf.getLastLogIndex()) || (args.LastLogTerm == rf.getLogTerm(rf.getLastLogIndex()) && args.LastLogIndex >= rf.getLastLogIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate && rf.role == FOLLOWER {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		//DPrintf("我的任期为 %d，我是第 %d号，我给第 %d号投票\n", rf.currentTerm, rf.me, rf.votedFor)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.resetElectionTimer()
		rf.persist()
		//rf.votedFor = -1
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.mu.Lock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.role = FOLLOWER
			rf.electionNum = 0
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			//rf.resetElectionTimer()
			rf.persist()
			rf.mu.Unlock()
			return ok
		}
		if reply.Term < rf.currentTerm {
			rf.mu.Unlock()
			return ok
		}
		if args.Term == rf.currentTerm {
			if reply.Success == true {
				//rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				//rf.nextIndex[server] = rf.matchIndex[server] + 1
				//DPrintf("TRUE:我是第%d号，nextIndex为%d", rf.me, rf.nextIndex[server])
				//DPrintf("nextIndex为%d", rf.nextIndex[server])
				//DPrintf("nextIndex为%d,matchIndex为%d,日志长度为%d", rf.nextIndex[server], rf.matchIndex[server], len(rf.log))
			}
			//if reply.Success == false && rf.nextIndex[server] > 1 {
			//	rf.nextIndex[server]--
			//}
			if reply.Success == false && rf.nextIndex[server] > 1 {
				if reply.XTerm == -1 {
					rf.nextIndex[server] = reply.XLen
					//DPrintf("FALSE:我是第%d号，我的日志长度为%d,nextIndex为%d", rf.me, len(rf.log), rf.nextIndex[server])
				} else {
					lastLogIndex := rf.findLastLogInTerm(reply.XTerm)
					//DPrintf("lastLogIndex为%d,reply的XIndex为%d,reply的任期为%d", lastLogIndex, reply.XIndex, reply.XTerm)
					if lastLogIndex > 0 {
						rf.nextIndex[server] = lastLogIndex
						//DPrintf("FALSE:我是第%d号，我的日志长度为%d,nextIndex为%d", rf.me, len(rf.log), rf.nextIndex[server])
					} else {
						rf.nextIndex[server] = max(1, reply.XIndex)
						//DPrintf("FALSE:我是第%d号，我的日志长度为%d,nextIndex为%d", rf.me, len(rf.log), rf.nextIndex[server])
					}
				}
				//DPrintf("FALSE:我是第%d号，我的日志长度为%d,nextIndex为%d", rf.me, len(rf.log), rf.nextIndex[server])
			}
		}
		//DPrintf("fuck,index为%d,rf.nextIndex[index]为%d", server, rf.nextIndex[server])
		rf.mu.Unlock()
	}
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//rf.mu.Lock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > args.Term {
			rf.role = FOLLOWER
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			//rf.resetElectionTimer()
			rf.persist()
			rf.mu.Unlock()
			return ok
		}
		if reply.Term < args.Term {
			rf.mu.Unlock()
			return ok
		}
		//if !reply.VoteGranted {
		//	rf.mu.Unlock()
		//	return ok
		//}
		if args.Term == rf.currentTerm && rf.role == CANDIDATE {
			if reply.VoteGranted {
				rf.electionNum++
				//DPrintf("当前任期为 %d,我是第 %d 号,我的投票数为 %d\n", rf.currentTerm, rf.me, rf.electionNum)
			}
			if rf.electionNum > len(rf.peers)/2 && rf.role == CANDIDATE {
				//DPrintf("我是第 %d号，我的任期为%d,我成为leader\n", rf.me, rf.currentTerm)
				rf.role = LEADER
				rf.resetElectionTimer()
				rf.electionNum = 0
				for i, _ := range rf.peers {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].LogIndex + 1
					rf.matchIndex[i] = 0
				}
				go rf.commitCheck()
			}
		}
		rf.mu.Unlock()
	}
	return ok
}
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		if rf.commitIndex < rf.lastIncludedIndex {
			rf.commitIndex = rf.lastIncludedIndex
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			//DPrintf("我是第%d号，rf.lastApplied为%d,rf.lastIncludedIndex为%d", rf.me, rf.lastApplied, rf.lastIncludedIndex)
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.lastIncludedIndex].Context, CommandIndex: rf.lastApplied}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			//DPrintf("我是第%d号,我的任期是%d,我提交了日志%d,内容是%v", rf.me, rf.currentTerm, rf.lastApplied, rf.log[rf.lastApplied-rf.lastIncludedIndex].Context)
		} else {
			rf.applyCond.Wait()
		}
	}
}
func (rf *Raft) commitCheck() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == LEADER {
			//DPrintf("我是第%d号，我的commit为%d，我的日志长度为%d", rf.me, rf.commitIndex, rf)
			for n := rf.commitIndex + 1; n <= rf.getLastLogIndex(); n++ {
				if rf.getLogTerm(n) != rf.currentTerm {
					continue
				}
				apply := 1
				for i := 0; i < len(rf.peers); i++ {
					if rf.me == i {
						continue
					}
					if rf.matchIndex[i] >= n {
						apply++
					}
					if apply > len(rf.peers)/2 {
						rf.commitIndex = n
						rf.applyCond.Broadcast()
						break
					}
				}
			}

			//apply := 1
			//for i := 0; i < len(rf.peers); i++ {
			//	if rf.me == i {
			//		continue
			//	}
			//	if rf.matchIndex[i] >= rf.commitIndex+1 {
			//		apply++
			//	}
			//}
			//if apply > len(rf.peers)/2 {
			//	rf.commitIndex = rf.commitIndex + 1
			//}
			//if rf.commitIndex > rf.lastApplied {
			//	rf.lastApplied++
			//	msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Context, CommandIndex: rf.lastApplied}
			//	rf.applyCh <- msg
			//}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.role != LEADER {
		return index, term, isLeader
	}
	index = rf.log[len(rf.log)-1].LogIndex + 1
	if len(rf.log) == 1 && rf.lastIncludedIndex != 0 {
		index = rf.lastIncludedIndex + 1
	}
	isLeader = true
	// Your code here (2B).
	rf.log = append(rf.log, Log{command, rf.currentTerm, index})
	rf.persist()
	//DPrintf("我是第%d号,添加log %d,任期为%d,内容为%v,我的提交为%d\n", rf.me, index, rf.currentTerm, command, rf.lastApplied)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	rf.electionTime = t.Add(time.Duration(150+rand.Intn(150)) * time.Millisecond)
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//rf.mu.Lock()
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.role = FOLLOWER
			rf.electionNum = 0
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			//rf.resetElectionTimer()
			rf.persist()
			rf.mu.Unlock()
			return ok
		}
		if reply.Term < rf.currentTerm {
			rf.mu.Unlock()
			return ok
		}
		if reply.Term == rf.currentTerm {
			rf.matchIndex[server] = args.LastIncludeIndex
			rf.nextIndex[server] = args.LastIncludeIndex + 1
			//DPrintf("server为%d,rf.nextIndex[server]为%d", server, rf.nextIndex[server])
			rf.mu.Unlock()
			return ok
		}
	}
	return ok
}
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.persist()
	}
	rf.role = FOLLOWER
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	if rf.lastIncludedIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		//DPrintf("我是第%d号，我正在安装日志，参数的rf.lastIncludedIndex为%d，我的rf.lastIncludedIndex为%d", rf.me, args.LastIncludeIndex, rf.lastIncludedIndex)
		return
	}
	index := args.LastIncludeIndex
	tempLog := make([]Log, 0)
	tempLog = append([]Log{{-1, 0, 0}}, tempLog...)
	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		tempLog = append(tempLog, rf.log[i-rf.lastIncludedIndex])
	}
	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.lastIncludedTerm = args.LastIncludeTerm
	rf.log = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)
	msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludeIndex, SnapshotTerm: args.LastIncludeTerm}
	rf.mu.Unlock()
	//DPrintf("我是第%d号，我安装了日志", rf.me)
	rf.applyCh <- msg
}
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == LEADER {
			for index := 0; index < len(rf.peers); index++ {
				if index == rf.me {
					rf.resetElectionTimer()
					//DPrintf("我是第 %d号,我的任期为 %d", rf.me, rf.currentTerm)
					continue
				}
				nextIndex := rf.nextIndex[index]
				//DPrintf("我是第%d号,nextIndex-1为%d", rf.me, nextIndex-1)
				//DPrintf("我是第%d号,rf.log[len(rf.log)-1].LogIndex为%d,index为%d,rf.nextIndex[index]为%d,rf.lastIncludedIndex为%d", rf.me, rf.log[len(rf.log)-1].LogIndex, index, rf.nextIndex[index], rf.lastIncludedIndex)
				if rf.nextIndex[index] <= rf.lastIncludedIndex {
					//DPrintf("我是第%d号，准备安装日志，我的rf.lastIncludedIndex为%d", rf.me, rf.lastIncludedIndex)
					args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.persister.ReadSnapshot()}
					reply := InstallSnapshotReply{}
					go rf.sendInstallSnapShot(index, &args, &reply)
					continue
				}
				lastLogIndex := rf.getLastLogIndex()
				prevLogIndex := rf.getLogIndex(nextIndex - 1)
				prevLogTerm := rf.getLogTerm(nextIndex - 1)
				//if rf.nextIndex[index]-1 == rf.lastIncludedIndex && rf.lastIncludedIndex != 0 {
				//	lastLogIndex = rf.lastIncludedIndex
				//	prevLogIndex = rf.lastIncludedIndex
				//	prevLogTerm = rf.lastIncludedTerm
				//}
				args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, make([]Log, lastLogIndex-rf.nextIndex[index]+1), rf.commitIndex, true}
				//DPrintf("prevlogIndex为%d,我是第%d号,entries的长度为%d,log长度为%d,lastlog为%d,next%d", prevLogIndex, rf.me, len(args.Entries), len(rf.log), lastLogIndex, rf.nextIndex[index])
				//args := AppendEntriesArgs{rf.currentTerm, rf.me, prevlog.LogIndex, prevlog.CurrentTerm, make([]Log, 0), rf.commitIndex, true}
				reply := AppendEntriesReply{}
				copy(args.Entries, rf.log[(rf.nextIndex[index]-rf.lastIncludedIndex):])
				//DPrintf("拷贝的log为%d\n", rf.log[rf.nextIndex[index]-rf.lastIncludedIndex].Context)
				go rf.sendAppendEntries(index, &args, &reply)
			}
		}
		if time.Now().After(rf.electionTime) && rf.role != LEADER {
			//rf.electionNum = 0
			rf.currentTerm++
			//DPrintf("我是第 %d号,我的任期为 %d", rf.me, rf.currentTerm)
			rf.role = CANDIDATE
			rf.votedFor = rf.me
			rf.resetElectionTimer()
			rf.electionNum = 1
			rf.persist()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				lastLogIndex := rf.log[len(rf.log)-1].LogIndex
				lastLogTerm := rf.log[len(rf.log)-1].CurrentTerm
				if lastLogIndex == 0 && rf.lastIncludedIndex != 0 {
					lastLogIndex = rf.lastIncludedIndex
					lastLogTerm = rf.lastIncludedTerm
				}
				args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
				reply := RequestVoteReply{}
				go rf.sendRequestVote(i, &args, &reply)
			}
			//DPrintf("当前任期为 %d，我是第 %d 号,我的投票数为 %d\n", rf.currentTerm, rf.me, electionNum)
			//if electionNum > len(rf.peers)/2 {
			//	rf.role = LEADER
			//	for i, _ := range rf.peers {
			//		rf.nextIndex[i] = rf.log[len(rf.log)-1].LogIndex + 1
			//		rf.matchIndex[i] = 0
			//	}
			//}
		}
		rf.mu.Unlock()

		//// pause for a random amount of time between 50 and 350
		//// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = FOLLOWER

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{-1, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 1; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
	rf.lastIncludedIndex = 0
	rf.currentTerm = 0
	rf.electionNum = 0
	rf.resetElectionTimer()
	//args := RequestVoteArgs{}
	//reply := RequestVoteReply{}
	//rf.sendRequestVote(rf.currentTerm, &args, &reply)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
