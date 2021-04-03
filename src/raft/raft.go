package raft

import (
	"fmt"
	"log"
	"math/rand"
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
	Data  string
	Term  int
	Index int
}

type Raft struct {
	mu    sync.RWMutex        // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

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
	log.Println(fmt.Sprintf("Vote Request Received at %d from %d", rf.me, args.CandidateId))
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	go rf.heartbeatTimeoutChecker()
	rf.lastHeartBeat = time.Now()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if server == rf.me {
		return false
	}
	log.Println(fmt.Sprintf("Send Vote Request %d from %d", server, rf.me))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok && reply.VoteGranted {
		log.Println(fmt.Sprintf("Vote Received at %d from %d", rf.me, server))
		rf.votesReceived++
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) heartbeatTimeoutChecker() {
	sleepTime := rand.Intn(500) + 500
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	rf.mu.RLock()
	lastHeartBeat := rf.lastHeartBeat
	lastVoteGranted := rf.lastVoteGranted
	rf.mu.RUnlock()
	if time.Since(lastHeartBeat) > time.Duration(sleepTime)*time.Millisecond && time.Since(lastVoteGranted) > time.Duration(sleepTime)*time.Millisecond {
		log.Println(fmt.Sprintf("LastHeartBeat at:%d, %d", rf.me, lastHeartBeat.UnixNano()/int64(time.Millisecond)))
		log.Println(fmt.Sprintf("Timeout at:%d, %d", rf.me, time.Now().UnixNano()/int64(time.Millisecond)))
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
	log.Println(fmt.Sprintf("Election Started By:%d", rf.me))
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.lastVoteGranted = time.Now()
	for i := 0; i < len(rf.peers); i++ {
		term, index := rf.lastLogInfo()
		args := RequestVoteArgs{rf.currentTerm, rf.me, term, index}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
	rf.mu.Unlock()
}

func (rf *Raft) electionTimeoutChecker() {
	sleepTime := rand.Intn(500) + 300
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	rf.mu.RLock()
	isElected := rf.votesReceived > len(rf.peers)/2
	isCandidate := rf.state == Candidate
	log.Println(fmt.Sprintf("Votes Received at %d:%d", rf.me, rf.votesReceived))
	rf.mu.RUnlock()
	if isElected {
		rf.elected()
	} else if isCandidate {
		rf.callElection()
	}
}

func (rf *Raft) elected() {
	rf.mu.Lock()
	rf.state = Leader
	log.Println(fmt.Sprintf("Elected: %d", rf.me))
	rf.mu.Unlock()
	go rf.sendHeartBeats()
}

func (rf *Raft) sendHeartBeats() {
	for _, isLeader := rf.GetState(); isLeader; {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.peers[i].Call("Raft.AppendEntries", &AppendEntriesArgs{}, &AppendEntriesReply{})
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	// Your code here (2B).

	return index, term, isLeader
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
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesReceived = 0
	go rf.heartbeatTimeoutChecker()
	return rf
}
