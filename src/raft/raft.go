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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// --- persisted state ---
	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term (or -1 if none)
	votedFor int
	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	log []Log

	// --- volatile state ---
	// do we need to store this??
	electionTimer  time.Time
	heartbeatTimer time.Time
	state          state
	quorum         int
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	// --- volatile state on leaders ---
	nextIndex  []int
	matchIndex []int
}

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

type state string

const (
	Candidate state = "Candidate"
	Follower  state = "Follower"
	Leader    state = "Leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.isleader()
}

func (rf *Raft) isleader() bool {
	return rf.state == Leader
}

func (rf *Raft) downgradeToFollowerState(term int, reason string) {
	PrintfWarn("%v Downgrading to follower state for term %v. reason: %v", rf.me, term, reason)
	rf.votedFor = -1
	rf.currentTerm = term
	rf.state = Follower
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.downgradeToFollowerState(args.Term, "stale term")
	}

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		// PrintfInfo("%v RequestVote: denying vote due to stale term %v", rf.me, args)
		reply.VoteGranted = false
		return
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// PrintfInfo("%v RequestVote: denying vote because node already voted for %v. %v", rf.me, rf.votedFor, args)
		reply.VoteGranted = false
		return
	}

	if rf.lastApplied > 0 && rf.log[rf.lastApplied].Index > args.LastLogIndex && rf.log[rf.lastApplied].Term > args.LastLogTerm {
		// PrintfInfo("%v RequestVote: denying vote due to stale logs %v", rf.me, args)
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	// PrintfInfo("%v RequestVote: %v", rf.me, reply)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reset election timer
	rf.electionTimer = time.Now()
	// PrintfInfo("%v received heartbeat from %v", rf.me, args.LeaderId)

	if args.Term > rf.currentTerm {
		rf.downgradeToFollowerState(args.Term, "stale term")
	}

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 2. Reply false if log doesn’t contain an entry at PrevLogIndex
	// whose term matches PrevLogTerm (§5.3)
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// --- TODO ---
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.heartbeatTimer = time.Now()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	cond := sync.NewCond(&rf.mu)
	successCount := 1
	// PrintfInfo("%v sending heartbeats to all", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int, mu *sync.Mutex) {
			request := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// Entries: [],
				PrevLogIndex: rf.commitIndex,
				PrevLogTerm:  rf.currentTerm,
			}
			var reply AppendEntriesReply

			rf.sendAppendEntries(server, &request, &reply)
			// PrintfInfo("%v sendAppendEntries %v, %v, %v", rf.me, i, &request, &reply)

			mu.Lock()
			if reply.Success {
				successCount += 1
				rf.electionTimer = time.Now()
			}
			cond.Broadcast()
			mu.Unlock()
		}(i, &rf.mu)
	}

	// wait for all RPCs
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for successCount < len(rf.peers) {
		cond.Wait()
	}

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	PrintfInfo("%v Start: %v", rf.me, command)

	// apply command to applyCh

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	PrintfError("%v got killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// PrintfDebug("%v ticker: %d", rf.me, rf.state)

		// • If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate:
		// convert to candidate
		if rf.state == Follower {
			// PrintfDebug("%v checking election timer: %v", rf.me, rf.electionTimer)
			if time.Since(rf.electionTimer) > 1000*time.Millisecond {
				PrintfWarn("%v term %v election timer expired, will attempt election soon...", rf.me, rf.currentTerm)
				rf.mu.Lock()
				rf.state = Candidate
				rf.electionTimer = time.Now()
				rf.mu.Unlock()

				go rf.attemptElection(rf.currentTerm + 1)
			}
		} else if rf.state == Leader {
			if time.Since(rf.heartbeatTimer) > 100*time.Millisecond {
				go rf.sendHeartbeats()
			}
			if time.Since(rf.electionTimer) > 1000*time.Millisecond {
				// downgrade leadership
				rf.mu.Lock()
				rf.downgradeToFollowerState(rf.currentTerm, "electionTimer has expired")
				rf.mu.Unlock()
			}
		}
	}
}

// • Increment currentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
func (rf *Raft) attemptElection(term int) {
	// sleep for random time
	ms := (rand.Int() % 50)
	// PrintfInfo("%v sleeping for %v", rf.me, ms)
	time.Sleep(time.Duration(ms) * time.Millisecond)

	rf.mu.Lock()
	currentState := rf.state
	rf.mu.Unlock()
	if currentState != Candidate {
		PrintfWarn("%v Skipping election as state has changed to %v", rf.me, currentState)
		return
	}

	PrintfInfo("%v attempting election for term: %v ", rf.me, term)

	// On conversion to candidate, start election.
	// TODO: If AppendEntries RPC received from new leader: convert to follower
	// TODO: If election timeout elapses: start new election
	rf.mu.Lock()
	rf.currentTerm = term
	rf.mu.Unlock()

	votesGranted := 0
	votesTotal := 0
	cond := sync.NewCond(&rf.mu)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.mu.Lock()
			rf.votedFor = rf.me
			votesGranted += 1
			votesTotal += 1
			rf.mu.Unlock()
			continue
		}

		go func(server int, mu *sync.Mutex) {
			var request RequestVoteArgs
			request.Term = rf.currentTerm
			request.CandidateId = rf.me

			if len(rf.log) > 0 {
				request.LastLogIndex = rf.log[len(rf.log)-1].Index
				request.LastLogTerm = rf.log[len(rf.log)-1].Term
			} else {
				request.LastLogIndex = -1
				request.LastLogTerm = -1
			}

			var reply RequestVoteReply
			rf.sendRequestVote(server, &request, &reply)

			mu.Lock()
			votesTotal += 1
			if reply.VoteGranted {
				votesGranted += 1
			}
			cond.Broadcast()
			mu.Unlock()

			PrintfInfo("%v received vote for term %v from %v. result: %v. votesGranted: %v", rf.me, request.Term, server, reply.VoteGranted, votesGranted)
		}(i, &rf.mu)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// wait till we get all votes
	for votesGranted < rf.quorum && votesTotal < len(rf.peers) {
		cond.Wait()
	}

	if rf.state != Candidate || rf.currentTerm != term {
		PrintfWarn("%v ignoring voting result as state or term has changed", rf.me)
		return
	}

	// If votes received from majority of servers: become leader
	// else, retry election
	if votesGranted >= rf.quorum {
		rf.state = Leader
		PrintfSuccess("------- %v won election for term %v -------", rf.me, rf.currentTerm)
		go rf.sendHeartbeats()
	} else {
		PrintfError("------- %v lost election for term %v. will retry for term %v -------", rf.me, rf.currentTerm, rf.currentTerm+1)
		go rf.attemptElection(rf.currentTerm + 1)
	}
}

// the service or tester wants to create a Raft server.
// the ports of all the Raft servers (including this one) are in peers[].
// this server's port is peers[me].
// all the servers' peers[] arrays have the same order.

// persister is a place for this server to save its persistent state,
// and also initially holds the most recent saved state, if any.

// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.

// Make() must return quickly, so it should start goroutines for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.quorum = (len(peers) / 2) + 1
	rf.persister = persister
	rf.me = me

	PrintfInfo("%v Make peers:%v, quorum:%v", rf.me, len(rf.peers), rf.quorum)

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	// rf.log = []string{}
	rf.electionTimer = time.Now().Add(-1 * time.Minute)
	// starts off as follower
	rf.state = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
