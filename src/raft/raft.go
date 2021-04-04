package raft

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerState int

const (
	Leader    ServerState = iota
	Candidate ServerState = iota
	Follower  ServerState = iota
)

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

type Raft struct {
	mu        sync.RWMutex // Lock to protect shared access to this peer's state
	applyChan chan ApplyMsg
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       ServerState
	currentTerm int
	votedFor    int
	logs        []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	votesReceived   int
	lastHeartBeat   time.Time
	lastVoteGranted time.Time
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) logsUpToDate(lastLogTerm int, lastLogIndex int) bool {
	term, index := rf.lastLogInfo()
	if lastLogTerm > term {
		return true
	} else if lastLogTerm == term && lastLogIndex >= index {
		return true
	}
	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.logsUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastVoteGranted = time.Now()
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	FirstIndex int
	Success    bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.FirstIndex = -1
		reply.Success = false
		return
	} else {
		if rf.state != Follower {
			rf.state = Follower
		}
		rf.currentTerm = args.Term
	}
	go rf.heartbeatTimeoutChecker()
	rf.lastHeartBeat = time.Now()

	if args.PrevLogIndex > len(rf.logs) || args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		var startIndex int
		if args.PrevLogIndex > len(rf.logs) {
			_, term := rf.lastLogInfo()
			reply.Term = term
			startIndex = len(rf.logs)
		} else {
			reply.Term = rf.logs[args.PrevLogIndex-1].Term
			startIndex = args.PrevLogIndex
		}
		reply.FirstIndex = 1
		for i := startIndex; i > 0; i-- {
			if rf.logs[i-1].Term != reply.Term {
				reply.FirstIndex = i + 1
				break
			}
		}
		reply.Success = false
		return
	}

	for _, entry := range args.Entries {
		if entry.Index <= len(rf.logs) && rf.logs[entry.Index-1].Term != entry.Term {
			rf.logs = rf.logs[:entry.Index-1]
		}
		if entry.Index > len(rf.logs) {
			rf.logs = append(rf.logs, entry)
		}
	}

	_, lastIndex := rf.lastLogInfo()
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyChan <- ApplyMsg{true, rf.logs[i-1].Command, i}
		}
	}
	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	for {
		if rf.killed() {
			return
		}
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if !ok {
			continue
		}
		rf.mu.Lock()
		if reply.VoteGranted {
			rf.votesReceived++
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
			}
		}
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) heartbeatTimeoutChecker() {
	sleepTime := rand.Intn(400) + 200
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	if rf.killed() {
		return
	}
	rf.mu.RLock()
	lastHeartBeat := rf.lastHeartBeat
	lastVoteGranted := rf.lastVoteGranted
	rf.mu.RUnlock()
	if time.Since(lastHeartBeat) > time.Duration(sleepTime)*time.Millisecond && time.Since(lastVoteGranted) > time.Duration(sleepTime)*time.Millisecond {
		rf.callElection()

	}
}

func (rf *Raft) lastLogInfo() (int, int) {
	index := 0
	term := 0
	if len(rf.logs) > 0 {
		lastLog := rf.logs[len(rf.logs)-1]
		index = lastLog.Index
		term = lastLog.Term
	}
	return term, index
}

func (rf *Raft) callElection() {
	go rf.electionTimeoutChecker()
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.lastVoteGranted = time.Now()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		term, index := rf.lastLogInfo()
		args := RequestVoteArgs{rf.currentTerm, rf.me, term, index}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
	rf.mu.Unlock()
}

func (rf *Raft) electionTimeoutChecker() {
	sleepTime := rand.Intn(400) + 200
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	if rf.killed() {
		return
	}
	rf.mu.RLock()
	isElected := rf.votesReceived > len(rf.peers)/2
	isCandidate := rf.state == Candidate
	rf.mu.RUnlock()
	if isElected {
		rf.elected()
	} else if isCandidate {
		rf.callElection()
	} else {
		go rf.heartbeatTimeoutChecker()
	}
}

func (rf *Raft) elected() {
	rf.mu.Lock()
	rf.state = Leader
	_, index := rf.lastLogInfo()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = index + 1
	}
	rf.mu.Unlock()
	go rf.sendHeartBeats()
}

func (rf *Raft) sendHeartBeats() {
	for _, isLeader := rf.GetState(); isLeader; {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.sendHeartBeat(i)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	if rf.killed() {
		return
	}
	rf.mu.RLock()
	prevTerm, prevIndex := rf.lastLogInfo()
	args := AppendEntriesArgs{rf.currentTerm, rf.me, prevTerm, prevIndex, []Log{}, rf.commitIndex}
	_, isLeader := rf.GetState()
	rf.mu.RUnlock()
	if !isLeader {
		return
	}
	rf.peers[server].Call("Raft.AppendEntries", &args, &AppendEntriesReply{})
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, -1, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevTerm, prevIndex := rf.lastLogInfo()
	index := rf.nextIndex[rf.me]
	newLog := Log{command, term, index}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me]++
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		_, lastIndex := rf.lastLogInfo()
		if lastIndex < rf.nextIndex[i] {
			continue
		}
		args := AppendEntriesArgs{rf.currentTerm, rf.me, prevTerm, prevIndex, []Log{newLog}, rf.commitIndex}
		go rf.sendAppendEntries(i, &args)
	}
	return index, term, isLeader
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	ok := false
	reply := AppendEntriesReply{Success: false}
	for !ok || !reply.Success {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		rf.mu.Lock()
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			vals := make([]int, len(rf.matchIndex))
			copy(vals, rf.matchIndex)
			sort.Ints(vals)
			majorityIndex := vals[len(vals)/2]
			if len(vals)%2 == 0 {
				majorityIndex = vals[len(vals)/2-1]
			}
			if majorityIndex > rf.commitIndex && rf.logs[majorityIndex-1].Term == rf.currentTerm {
				rf.commitIndex = majorityIndex
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					rf.applyChan <- ApplyMsg{true, rf.logs[i-1].Command, i}
				}
			}
		} else {
			if reply.FirstIndex > 0 {
				args.PrevLogIndex = reply.FirstIndex - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				args.Entries = []Log{}
				for i := args.PrevLogIndex + 1; i <= rf.matchIndex[rf.me]; i++ {
					args.Entries = append(args.Entries, rf.logs[i-1])
				}
			} else {
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyChan = applyCh
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesReceived = 0
	rf.lastHeartBeat = time.Time{}
	rf.lastVoteGranted = time.Time{}
	go rf.heartbeatTimeoutChecker()
	return rf
}
