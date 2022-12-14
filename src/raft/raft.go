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

	"fmt"
	"log"
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
	applyCh   chan ApplyMsg

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

	newLogEntries bool // indicates that there are new log entries which aren't sent to peers
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
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isleader()
	rf.mu.Unlock()

	return term, isleader
}

// Update Leader's election timer if it win's atleast 1 vote
// Update election timer once peer becomes a candidate

// Update follower's election timer if:
// 1. Vote is granted to a candidate in a RequestVote RPC call.
// 2. AppendEntries RPC has the same or greater term.
func (rf *Raft) updateElectionTimer() {
	rf.electionTimer = time.Now()
}

func (rf *Raft) isleader() bool {
	return rf.state == Leader
}

func (rf *Raft) lastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) logTerm(index int) int {
	if index < 1 || index > len(rf.log)-1 {
		return 0
	} else {
		return rf.log[index].Term
	}
}

func (rf *Raft) stepDown(term int, reason string) {
	PrintfWarn("%v Stepping down to follower for term %v. reason: %v", rf.me, term, reason)
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
	PrintfPurple("%v RequestVote received for CandidateId:%v for Term: %v", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this is crucial for TestReElection2A
	// when 2 candidates are requesting votes for different terms. the one with the older term has to step down and update term.
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term, "detected stale term in RequestVote")
	}

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		PrintfWarn("%v RequestVote: denying vote to %v due to stale term %v", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	}

	// Reject voke if receiver has already voted in this term
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		PrintfWarn("%v RequestVote: denying vote to %v for Term: %v because receiver already voted for %v.", rf.me, args.CandidateId, args.Term, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	// Reject voke if candidate’s log is stale
	if (args.LastLogIndex < len(rf.log)-1) || rf.logTerm(rf.lastApplied) > args.LastLogTerm {
		PrintfWarn("%v RequestVote: denying vote to %v due to stale logs", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	PrintfInfo("%v granting vote to: %v for %v (args.LastLogIndex: %v)", rf.me, args.CandidateId, reply.Term, args.LastLogIndex)

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	// UpdateElectionTimer if vote is granted to a candidate in a RequestVote RPC call.
	rf.updateElectionTimer()
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
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

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term, fmt.Sprintf("detected stale term from AppendEntries (%v is already the leader)", args.LeaderId))
	}

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		// PrintfWarn("%v AppendEntries: denying from %v due to stale term %v", rf.me, args.LeaderId, args.Term)
		reply.Success = false
		return
	}

	// Update election timer if RPC has the same or greater term.
	rf.updateElectionTimer()

	// 2. Reply false if log doesn’t contain an entry at PrevLogIndex whose term matches PrevLogTerm (§5.3)
	// --- Very Important Consistency check! ---
	// Compare the index and term of the last entries in the logs to determine which of two logs is more up-to-date.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date
	// ---
	if args.PrevLogIndex > rf.lastIndex() || rf.logTerm(rf.lastIndex()) != args.PrevLogTerm {
		// PrintfWarn("%v AppendEntries: denying from %v due to stale logs rf.lastIndex: %v, args.PrevLogIndex: %v", rf.me, args.LeaderId, rf.lastIndex(), args.PrevLogIndex)
		reply.Success = false
		return
	}

	// PrintfWarn("%v AppendEntries from %v. rf.lastIndex: %v, rf.logTerm(rf.lastIndex()): %v, args.PrevLogIndex: %v", rf.me, args.LeaderId, rf.lastIndex(), rf.logTerm(rf.lastIndex()), args.PrevLogTerm)

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if len(args.Entries) > 0 && rf.logTerm(args.PrevLogIndex+1) != args.Entries[0].Term {
		// PrintfWarn("%v before pop: %v", rf.me, rf.log)
		rf.log = rf.log[:args.PrevLogIndex+1]
		// PrintfWarn("%v after pop: %v", rf.me, rf.log)
	}

	curr_index := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		// ignore log entries if already present in its log
		// This makes appendEntries requests idempotent
		// required for TestConcurrentStarts2B
		if curr_index < len(rf.log) {
			continue
		}

		if len(rf.log) != args.Entries[i].Index {
			log.Fatalf("%v trying to apply Log %v at wrong index %v in %v", rf.me, args.Entries[i], len(rf.log), rf.log)
		}

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries[i])
		// PrintfSuccess("%v:%v append(%v)", rf.me, rf.state, args.Entries[i])

		curr_index++
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := rf.commitIndex
		if args.LeaderCommit < rf.lastIndex() {
			newCommitIndex = args.LeaderCommit
		} else {
			newCommitIndex = rf.lastIndex()
		}

		for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: rf.log[i].Index,
			}
			rf.applyCh <- applyMsg
			rf.commitIndex++
			// PrintfSuccess("%v:%v commited(%v). commitIndex: %v", rf.me, rf.state, rf.log[i], rf.commitIndex)
		}

		rf.commitIndex = newCommitIndex
	}

	PrintfPurple("%v:%v log: %v. commitIndex: %v", rf.me, rf.state, rf.log, rf.commitIndex)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesToAllPeers() bool {
	cond := sync.NewCond(&rf.mu)
	successCount := 1
	totalCount := 1
	// TODO: set a max batch size
	rf.mu.Lock()
	lastIndexSentToPeers := rf.lastIndex()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int, mu *sync.Mutex) {
			mu.Lock()
			// PrintfDebug("%v b4.sendAppendEntries to %v rf.nextIndex: [%v:%v]", rf.me, server, rf.nextIndex[server], lastIndexSentToPeers+1)
			entries := []Log{}
			if rf.nextIndex[server] <= lastIndexSentToPeers {
				entries = rf.log[rf.nextIndex[server] : lastIndexSentToPeers+1]
			}
			request := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,             // index of log entry immediately preceding new ones
				PrevLogTerm:  rf.logTerm(rf.nextIndex[server] - 1), // term of prevLogIndex entry
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			// PrintfInfo("%v b4.sendAppendEntries to %v request:%v", rf.me, server, request)
			mu.Unlock()

			reply := AppendEntriesReply{}
			result := rf.sendAppendEntries(server, &request, &reply)
			PrintfInfo("%v sendAppendEntries to %v, %v, %v", rf.me, server, &request, &reply)

			mu.Lock()
			totalCount += 1
			if !result {
				// PrintfWarn("%v sendAppendEntries request to %v failed", rf.me, server)
			} else {
				if rf.currentTerm < reply.Term {
					rf.stepDown(reply.Term, "detected stale term from AppendEntries.Reply")
				}

				if rf.state == Leader {
					if reply.Success {
						successCount += 1
						rf.updateElectionTimer()

						// If successful: update nextIndex and matchIndex for follower (§5.3)
						rf.matchIndex[server] += len(entries)
						rf.nextIndex[server] += len(entries)

						// PrintfDebug("%v updated rf.nextIndex: %v", rf.me, rf.nextIndex)
					} else {
						// If a follower’s log is inconsistent with the leader’s,
						// the AppendEntries consistency check will fail in the next AppendEntries RPC.
						// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
						// Eventually nextIndex will reach a point where the leader and follower logs match.

						// When this happens, AppendEntries will succeed, which removes any conflicting entries in the follower’s log
						// and appends entries from the leader’s log (if any).

						// Once AppendEntries succeeds, the follower’s log is consistent with the leader’s,
						// and it will remain that way for the rest of the term.
						if rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
						}
					}
				}
			}
			cond.Broadcast()
			mu.Unlock()

		}(i, &rf.mu)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// wait for all RPCs
	for totalCount < len(rf.peers) {
		cond.Wait()

		// commit whenever we get a quorum
		if successCount >= rf.quorum {
			oldCommitIndex := rf.commitIndex
			for i := oldCommitIndex + 1; i <= lastIndexSentToPeers; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: rf.log[i].Index,
				}
				rf.applyCh <- applyMsg
				rf.commitIndex++
				// PrintfSuccess("%v:%v commited(%v). commitIndex: %v", rf.me, rf.state, rf.log[i], rf.commitIndex)
			}
		}

		PrintfPurple("%v:%v log: %v. commitIndex: %v", rf.me, rf.state, rf.log, rf.commitIndex)
	}

	return true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	isLeader := rf.isleader()

	// return false if its not the leader
	if !isLeader {
		PrintfInfo("%v:%v Start(%v). result: false", rf.me, rf.state, command)
		return -1, currentTerm, false
	}

	// Step 1: append entry to local log as a new entry,
	// index at which log entry will be written. starts from 1
	newLogIndex := rf.nextIndex[rf.me]
	newLog := Log{
		Command: command,
		Term:    currentTerm,
		Index:   newLogIndex,
	}
	rf.log = append(rf.log, newLog)
	rf.nextIndex[rf.me]++
	rf.newLogEntries = true
	PrintfInfo("%v:%v Start(%v). result: true. newLogIndex: %v", rf.me, rf.state, command, newLogIndex)
	PrintfPurple("%v:%v log: %v. commitIndex: %v", rf.me, rf.state, rf.log, rf.commitIndex)

	// Step 2: (This will happen in rf.sendAppendEntriesToAllPeers() in the next trigger loop)
	// issue AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
	// If followers crash or run slowly, or if network packets are lost,
	// the leader retries AppendEntries RPCs indefinitely (even after it has responded to the client)
	// until all followers eventually store all log entries.

	return newLogIndex, currentTerm, true
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

		// • If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate:
		// convert to candidate
		rf.mu.Lock()
		// PrintfDebug("%v ticker: %v", rf.me, rf.state)
		if rf.state == Follower {
			// PrintfDebug("%v checking election timer: %v", rf.me, rf.electionTimer)
			if time.Since(rf.electionTimer) > 1000*time.Millisecond {
				PrintfWarn("%v term %v election timer expired, will attempt election soon...", rf.me, rf.currentTerm)
				rf.state = Candidate
				// Update election timer once peer becomes a candidate
				rf.updateElectionTimer()

				go rf.attemptElection(rf.currentTerm + 1)
			}
		} else if rf.state == Leader {
			if rf.newLogEntries || time.Since(rf.heartbeatTimer) > 200*time.Millisecond {
				// Update election timer before sending heartbeat
				// PrintfInfo("%v sendAppendEntriesToAllPeers: %v", rf.me, time.Since(rf.heartbeatTimer))
				rf.newLogEntries = false
				rf.heartbeatTimer = time.Now()
				go rf.sendAppendEntriesToAllPeers()
			}
			if time.Since(rf.electionTimer) > 1000*time.Millisecond {
				// downgrade leadership
				rf.stepDown(rf.currentTerm, "electionTimer has expired")
			}
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

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

	electionStartTime := time.Now()

	PrintfInfo("%v attempting election for term: %v ", rf.me, term)

	// On conversion to candidate, start election.
	// TODO: If AppendEntries RPC received from new leader: convert to follower
	// TODO: If election timeout elapses: start new election
	rf.mu.Lock()
	rf.currentTerm = term
	rf.votedFor = rf.me
	votesGranted := 1
	votesTotal := 1
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int, mu *sync.Mutex) {
			request := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastIndex(),
				LastLogTerm:  rf.logTerm(rf.lastIndex()),
			}

			var reply RequestVoteReply
			// PrintfInfo("%v sendRequestVote for term %v to %v.", rf.me, request.Term, server)
			result := rf.sendRequestVote(server, &request, &reply)

			mu.Lock()
			votesTotal += 1
			if !result {
				PrintfWarn("%v sendRequestVote request to %v failed", rf.me, server)
			} else {
				if reply.VoteGranted {
					votesGranted += 1
				}
				PrintfInfo("%v received vote for term %v from %v. result: %v. votesGranted: %v", rf.me, request.Term, server, reply, votesGranted)
			}
			cond.Broadcast()
			mu.Unlock()

		}(i, &rf.mu)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// wait till we get all votes or get enough upvotes
	// NOTE: this loop needs to wait for either (votesGranted or votesTotal)
	for votesGranted < rf.quorum && votesTotal < len(rf.peers) {
		cond.Wait()
		if rf.state != Candidate || rf.currentTerm != term {
			PrintfWarn("%v ignoring voting result as state or term has changed", rf.me)
			return
		}

		if time.Since(electionStartTime) > 500*time.Millisecond {
			PrintfDebug("%v time since electionStartTime: %v", rf.me, time.Since(electionStartTime))
			break
		}
	}

	// If votes received from majority of servers: become leader
	// else, retry election
	if votesGranted >= rf.quorum {
		PrintfSuccess("------- %v won election for term %v -------", rf.me, rf.currentTerm)
		rf.state = Leader

		// When a leader first comes to power,
		// it initializes all nextIndex values to the index just after the last one in its log
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastIndex() + 1
		}

		go rf.sendAppendEntriesToAllPeers()
	} else {
		PrintfError("------- %v lost election for term %v -------", rf.me, rf.currentTerm)
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

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.quorum = (len(peers) / 2) + 1
	rf.persister = persister
	rf.me = me

	PrintfInfo("%v Make peers:%v, quorum:%v", rf.me, len(rf.peers), rf.quorum)

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.electionTimer = time.Now().Add(-1 * time.Minute)
	// starts off as follower
	rf.state = Follower
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 1
	}

	newLog := Log{
		Command: -1,
		Term:    0,
		Index:   0,
	}
	rf.log = []Log{newLog}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
