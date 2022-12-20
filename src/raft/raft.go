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

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	CurrentTerm int
	// candidateId that received vote in current term (or -1 if none)
	VotedFor int
	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	Log []Log

	// shapshot
	LastIncludedIndex int
	LastIncludedTerm  int

	// --- volatile state ---
	// do we need to store this??
	electionTimer  time.Time
	heartbeatTimer time.Time
	state          state
	quorum         int

	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	// Why are commitIndex volatile?
	// commitIndex is volatile because Raft can figure out a correct value for it after a reboot using just the persistent state.
	// Once a leader successfully gets a new log entry committed, it knows everything before that point is also committed.
	// A follower that crashes and comes back up will be told about the right commitIndex whenever the current leader sends it an AE.
	commitIndex int

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Why are lastApplied volatile?
	// lastApplied starts at zero after a reboot because the basic Raft algorithm assumes the service (e.g., a key/value database) doesn’t keep any persistent state.
	// Thus its state needs to be completely recreated by replaying all log entries.
	lastApplied int

	// --- volatile state on leaders ---
	nextIndex  []int
	matchIndex []int

	newLogEntries bool // indicates that there are new log entries which aren't sent to peers

	appendEntriesRequestId int
}

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

type state string

const (
	Candidate          state = "Candidate"
	Follower           state = "Follower"
	Leader             state = "Leader"
	electionTimeoutMax int   = 1200
	electionTimeoutMin int   = 800
	heartbeatInterval  int   = 100
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.isleader()
	rf.mu.Unlock()

	return term, isleader
}

func RandRange(from, to int) int {
	return rand.Intn(to-from) + from
}

// set fast election timeout on startup, with some randomness
func (rf *Raft) initElectionTimer() {
	rf.electionTimer = time.Now().Add(time.Duration(RandRange(0, electionTimeoutMax-electionTimeoutMin)) * time.Millisecond)
}

func (rf *Raft) updateElectionTimer() {
	diff := time.Duration(RandRange(electionTimeoutMin, electionTimeoutMax)) * time.Millisecond
	rf.electionTimer = time.Now().Add(diff)
	// PrintfDebug("%v updateElectionTimer: diff:%v. time:%v", rf.me, diff, rf.electionTimer)
}

func (rf *Raft) updateHeartBeatTimer(triggerHeartbeats bool) {
	if triggerHeartbeats {
		rf.heartbeatTimer = time.Now().Add(-1 * time.Minute)
	} else {
		rf.heartbeatTimer = time.Now().Add(time.Duration(heartbeatInterval) * time.Millisecond)
	}
	// PrintfDebug("%v heartbeatTimer:%v", rf.me, rf.heartbeatTimer)
}

func (rf *Raft) isleader() bool {
	return rf.state == Leader
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.Log) == 0 {
		return rf.LastIncludedIndex
	}

	return rf.Log[len(rf.Log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.Log) == 0 {
		return rf.LastIncludedTerm
	}

	return rf.Log[len(rf.Log)-1].Term
}

func (rf *Raft) translateIndex(index int) int {
	// PrintfDebug("%v index translation: before:%v , after:%v", rf.me, index, index-rf.LastIncludedIndex-1)
	return index - rf.LastIncludedIndex - 1
}

func (rf *Raft) log(index int) Log {
	translatedIndex := rf.translateIndex(index)

	if translatedIndex < 0 || translatedIndex >= len(rf.Log) {
		panic(fmt.Sprintf("%v invalid Index:%v=translateIndex(%v) for %v", rf.me, translatedIndex, index, rf.Log))
	}

	return rf.Log[translatedIndex]
}

func (rf *Raft) logTerm(index int) int {
	translatedIndex := rf.translateIndex(index)

	if translatedIndex < 0 || translatedIndex >= len(rf.Log) {
		return rf.LastIncludedTerm
	}

	return rf.Log[translatedIndex].Term
}

func (rf *Raft) stepDown(term int, reason string) {
	PrintfWarn("%v Stepping down to Follower from %v for term %v. reason: %v", rf.me, rf.state, term, reason)
	rf.VotedFor = -1
	rf.CurrentTerm = term
	rf.state = Follower
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []Log
	var LastIncludedIndex int
	var LastIncludedTerm int

	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil || d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil {
		// throw error...
		PrintfError("readPersist error?")
	}
	PrintfSuccess("%v readPersist: T:%v. VF:%v. LS:{I:%v,T:%v}", rf.me, CurrentTerm, VotedFor, LastIncludedIndex, LastIncludedTerm)

	rf.CurrentTerm = CurrentTerm
	rf.VotedFor = VotedFor
	rf.Log = Log

	rf.LastIncludedTerm = LastIncludedTerm

	rf.LastIncludedIndex = LastIncludedIndex
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = rf.LastIncludedIndex
		rf.nextIndex[i] = rf.LastIncludedIndex + 1
	}
	PrintfSuccess("%v rf.nextIndex(%v)", rf.me, rf.nextIndex)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// No need to to implement this

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(lastIncludedIndex int, snapshot []byte) {
	go rf.processSnapshot(lastIncludedIndex, snapshot)
}

func (rf *Raft) processSnapshot(lastIncludedIndex int, snapshot []byte) {
	// PrintfWarn("%v Snapshot start: lastIncludedIndex:%v. %v", rf.me, lastIncludedIndex, reflect.ValueOf(&rf.mu).Elem().FieldByName("state"))
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.LastIncludedIndex {
		PrintfWarn("%v Ignore Snapshot(%v) as Log has already been trimmed upto %v", rf.me, lastIncludedIndex, rf.LastIncludedIndex)
		return
	}

	translatedLastIncludedIndex := rf.translateIndex(lastIncludedIndex)
	lastIncludedTerm := rf.Log[translatedLastIncludedIndex].Term

	if translatedLastIncludedIndex+1 > -1 {
		PrintfWarn("%v Trimming Log after Snapshot: \nbefore:%v, \nafter:%v", rf.me, rf.Log, rf.Log[translatedLastIncludedIndex+1:])
		rf.Log = rf.Log[translatedLastIncludedIndex+1:]

		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	}

	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm
	PrintfWarn("%v Snapshot end: lastIncludedIndex:%v, lastIncludedTerm:%v", rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm)
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
	// PrintfWarn("%v received RequestVote from %v for term %v", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// this is crucial for TestReElection2A
	// when 2 candidates are requesting votes for different terms. the one with the older term has to step down and update term.
	// if rf.state == Candidate && args.Term > rf.CurrentTerm {
	if args.Term > rf.CurrentTerm {
		rf.stepDown(args.Term, "detected stale term in RequestVote")
	}

	reply.Term = rf.CurrentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		PrintfWarn("%v:%v:%v RequestVote: denying vote to %v due to stale term %v", rf.me, rf.state, rf.CurrentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	}

	// Reject voke if receiver has already voted in this term
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		PrintfWarn("%v:%v:%v RequestVote: denying vote to %v for Term:%v because receiver already voted for %v.", rf.me, rf.state, rf.CurrentTerm, args.CandidateId, args.Term, rf.VotedFor)
		reply.VoteGranted = false
		return
	}

	// Reject voke if candidate’s log is not "up-to-date":
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// See (§5.4 figure 8)

	// Part 1: If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	if rf.lastLogTerm() > args.LastLogTerm {
		PrintfWarn("%v:%v:%v RequestVote: denying vote to %v for Term:%v due to stale logs (log-term mismatch). args.LastLogTerm: %v.", rf.me, rf.state, rf.CurrentTerm, args.CandidateId, args.Term, args.LastLogTerm)
		reply.VoteGranted = false
		return
	}

	// Part 2: If the logs end with the same term, then whichever log is longer is more up-to-date.
	if args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex < rf.lastLogIndex() {
		PrintfWarn("%v RequestVote: denying vote to %v due to stale logs (log-index mismatch). args.LastLogIndex: %v", rf.me, args.CandidateId, args.LastLogIndex)
		reply.VoteGranted = false
		return
	}

	PrintfInfo("%v granting vote to: %v for term:%v (args.LastLogIndex: %v, args.LastLogTerm: %v) \nrf.log:%v", rf.me, args.CandidateId, reply.Term, args.LastLogIndex, args.LastLogTerm, rf.Log)

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	rf.VotedFor = args.CandidateId
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
	RequestId    int
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int
	ConflictTerm  int
	MatchIndex    int
}

func (x *AppendEntriesArgs) toString() string {
	return fmt.Sprintf("{R.Id:%v. T:%v. L.Id:%v. PL:{I:%v, T:%v}. LC:%v. E:%v}", x.RequestId, x.Term, x.LeaderId, x.PrevLogIndex, x.PrevLogTerm, x.LeaderCommit, x.Entries)
}

func (x *AppendEntriesReply) toString() string {
	return fmt.Sprintf("{T:%v. S:%v. CL:{I:%v, T:%v}. LI:%v.}", x.Term, x.Success, x.ConflictIndex, x.ConflictTerm, x.MatchIndex)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term > rf.CurrentTerm {
		rf.stepDown(args.Term, fmt.Sprintf("detected stale term from AppendEntries (%v is already the leader)", args.LeaderId))
	}

	reply.Term = rf.CurrentTerm
	reply.MatchIndex = rf.lastLogIndex()
	reply.ConflictTerm = 0
	reply.ConflictIndex = 0

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		// PrintfWarn("%v denying AppendEntries from %v due to stale term %v", rf.me, args.LeaderId, args.Term)
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

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		PrintfWarn("%v denying AppendEntries from %v due to incomplete logs. args.PrevLogIndex: %v, rf.lastIndex: %v. \nrf.lastLogIndex:%v", rf.me, args.LeaderId, args.PrevLogIndex, rf.lastLogIndex(), rf.lastLogIndex())
		return
	}

	if rf.logTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false

		// Optimization to reduce the number of rejected AppendEntries RPCs: (§5.3, page 7 first para)
		// when rejecting an AppendEntries request, the follower can include the term of the conflicting entry and the first index it stores for that term.
		// With this information, the leader can decrement nextIndex to bypass all of the conflicting entries in that term;
		// one AppendEntries RPC will be required for each term with conflicting entries, rather than one RPC per entry.
		reply.ConflictTerm = rf.logTerm(args.PrevLogIndex)
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.logTerm(i) == reply.ConflictTerm {
				reply.ConflictIndex = i
			} else {
				break
			}
		}
		PrintfWarn("%v denying AppendEntries from %v due to log-term-mismatch. args.PrevLogIndex: %v, args.PrevLogTerm: %v. \nargs.Entries: %v, \nrf.log: %v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.Log)
		return
	}

	// PrintfWarn("%v AppendEntries from %v. rf.lastIndex: %v, rf.lastLogTerm(): %v, args: %v. \nLogs:%v", rf.me, args.LeaderId, rf.lastLogIndex(), rf.lastLogTerm(), args.toString(), rf.Log)

	translated_curr_index := rf.translateIndex(args.PrevLogIndex + 1)
	for i := 0; i < len(args.Entries); i++ {
		if translated_curr_index < 0 {
			translated_curr_index++
			continue
		}

		if translated_curr_index < len(rf.Log) {
			if rf.Log[translated_curr_index].Term != args.Entries[i].Term {
				// 3. If an existing entry conflicts with a new one (same index but different terms),
				// delete the existing entry and all that follow it (§5.3)
				// related tests: TestRejoin2B, TestUnreliableAgree2C
				temp := rf.Log[:translated_curr_index]
				PrintfWarn("%v Trimming Log: before:%v, after:%v", rf.me, rf.Log, temp)
				rf.Log = temp
			} else {
				// make appendEntries requests idempotent, by ignoring log entries if already present in its log
				// required for TestConcurrentStarts2B
				PrintfDebug("%v ignoring %v since its already present in the log", rf.me, args.Entries[i])
				translated_curr_index++
				continue
			}
		}

		if len(rf.Log) >= 1 {
			if rf.Log[translated_curr_index-1].Index+1 != args.Entries[i].Index {
				log.Fatalf("%v trying to apply invalid Log %v in %v", rf.me, args.Entries[i], rf.Log)
			}
			if rf.Log[translated_curr_index-1].Command == args.Entries[i].Command {
				log.Fatalf("%v trying to apply duplicate Log %v in %v", rf.me, args.Entries[i], rf.Log)
			}
		}

		// 4. Append any new entries not already in the log
		rf.Log = append(rf.Log, args.Entries[i])

		translated_curr_index++
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
	}
	PrintfPurple("%v:%v log: %v. commitIndex: %v", rf.me, rf.state, rf.Log, rf.commitIndex)

	go rf.applyLogs()

	reply.MatchIndex = rf.lastLogIndex()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesToAllPeers() {
	rf.mu.Lock()
	rf.updateHeartBeatTimer(false)
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(i, 0)
	}
}

func (rf *Raft) sendAppendEntriesToPeer(server int, retryCount int) {
	if retryCount > 2 {
		return
	}

	rf.mu.Lock()
	// TODO: set a max batch size
	entries := []Log{}

	if rf.nextIndex[server] <= rf.lastLogIndex() {
		if rf.nextIndex[server] <= rf.LastIncludedIndex {
			PrintfSuccess("%v sending InstallSnapshot to %v as logs have been snapshotted! rf.nextIndex[server]:%v, rf.LastIncludedIndex:%v.", rf.me, server, rf.nextIndex[server], rf.LastIncludedIndex)
			go rf.createAndSendInstallSnapshot(server)
		} else {
			translatedNextIndex := rf.translateIndex(rf.nextIndex[server])
			translatedLastLogIndex := rf.translateIndex(rf.lastLogIndex())

			// PrintfDebug("%v left:%v. right:%v.", rf.me, translatedNextIndex, translatedLastLogIndex+1)
			entries = rf.Log[translatedNextIndex : translatedLastLogIndex+1]
		}
	}

	request := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,             // index of log entry immediately preceding new ones
		PrevLogTerm:  rf.logTerm(rf.nextIndex[server] - 1), // term of prevLogIndex entry
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
		RequestId:    rf.appendEntriesRequestId,
	}
	rf.appendEntriesRequestId++
	PrintfInfo("%v b4.sendAppendEntries to %v. retryCount:%v. request:%v", rf.me, server, retryCount, request.toString())

	current_state := rf.state
	rf.mu.Unlock()

	if current_state != Leader {
		PrintfWarn("%v not sendAppendEntries to %v as state has changed.", rf.me, server)
	}

	reply := AppendEntriesReply{}
	result := rf.sendAppendEntries(server, &request, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		if !result {
			PrintfError("%v sendAppendEntries to %v timed-out! retryCount:%v. %v, %v.", rf.me, server, retryCount, request.toString(), &reply)
			go rf.sendAppendEntriesToPeer(server, retryCount+1)
		} else {
			if rf.CurrentTerm < reply.Term {
				rf.stepDown(reply.Term, "detected stale term from AppendEntries.Reply")
			} else {
				if reply.Success {
					rf.updateElectionTimer()

					// If successful: update nextIndex and matchIndex for follower (§5.3)
					rf.matchIndex[server] = reply.MatchIndex
					rf.nextIndex[server] = reply.MatchIndex + 1

					rf.commitLogs()

					PrintfSuccess("%v sendAppendEntries to %v succeeded. retryCount:%v. %v. matchIndex: %v", rf.me, server, retryCount, request.toString(), rf.matchIndex)
				} else {
					// If a follower’s log is inconsistent with the leader’s,
					// the AppendEntries consistency check will fail in the next AppendEntries RPC.
					// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
					// Eventually nextIndex will reach a point where the leader and follower logs match.

					// When this happens, AppendEntries will succeed, which removes any conflicting entries in the follower’s log
					// and appends entries from the leader’s log (if any).

					// Once AppendEntries succeeds, the follower’s log is consistent with the leader’s,
					// and it will remain that way for the rest of the term.
					PrintfWarn("%v sendAppendEntries to %v was denied! retryCount:%v. %v, %v.", rf.me, server, retryCount, request.toString(), reply.toString())

					if reply.ConflictIndex == 0 {
						rf.nextIndex[server] = reply.MatchIndex + 1
					} else {
						rf.nextIndex[server] = reply.ConflictIndex
					}
					rf.matchIndex[server] = reply.MatchIndex

					go rf.sendAppendEntriesToPeer(server, retryCount+1)
				}
				PrintfDebug("%v updated rf.matchIndex:%v. rf.nextIndex: %v", rf.me, rf.matchIndex, rf.nextIndex)
			}
		}
	} else {
		// PrintfWarn("%v ignoring result from sendAppendEntries to %v as state has changed, %v, %v.", rf.me, server, request.toString(), reply.toString())
	}

	rf.persist()
}

// The leader decides when it is safe to apply a log entry to the state machines when:
// - a majority of peers have applied the log
// - log belong to the same term

// what happens to replicated but uncommited logs ??
// supposed a 3-member raft cluster a[master],b,c
// client send command to a, a replicate it to b and c, a apply the log to the status machine and response to client, then crash before replicate the committed state to b and c.
// Ans: The next leader will commit those entries.
// https://groups.google.com/g/raft-dev/c/n8YledqIrUs
func (rf *Raft) commitLogs() {
	if rf.lastLogIndex() <= rf.commitIndex {
		return
	}
	PrintfSuccess("%v:%v current commitIndex: %v. rf.lastLogIndex(): %v. rf.matchIndex: %v", rf.me, rf.state, rf.commitIndex, rf.lastLogIndex(), rf.matchIndex)

	for n := rf.commitIndex + 1; n <= rf.lastLogIndex(); n++ {
		if rf.logTerm(n) <= rf.CurrentTerm {
			cnt := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt++
				}
			}

			if cnt >= rf.quorum {
				rf.commitIndex = n
				PrintfSuccess("%v:%v updated commitIndex: %v", rf.me, rf.state, rf.commitIndex)
			}
		}
	}

	go rf.applyLogs()
}

func (rf *Raft) applyLogs() {
	applyMsgs := []ApplyMsg{}
	rf.mu.Lock()
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		log := rf.log(rf.lastApplied)
		applyMsgs = append(applyMsgs, ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.Index,
		})
		PrintfSuccess("%v:%v applied(%v). lastApplied: %v", rf.me, rf.state, log, rf.lastApplied)
	}
	for _, applyMsg := range applyMsgs {
		rf.applyCh <- applyMsg
	}
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	RequestId         int

	// Snapshots are split into chunks for transmission;
	// this gives the follower a sign of life with each chunk, so it can reset its election timer.
	Done   int // true if this is the last chunk
	Offset int // byte offset where chunk is positioned in the snapshot file
}

type InstallSnapshotReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // is this needed ??
}

func (x *InstallSnapshotArgs) toString() string {
	return fmt.Sprintf("{R.Id:%v. T:%v. L.Id:%v. LI:{I:%v, T:%v}. Offset:%v. Done:%v}", x.RequestId, x.Term, x.LeaderId, x.LastIncludedIndex, x.LastIncludedTerm, x.Offset, x.Done)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	PrintfWarn("%v:%v:%v received InstallSnapshot from %v", rf.me, rf.state, rf.CurrentTerm, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.CurrentTerm {
		PrintfWarn("%v Rejecting Snapshot from %v. args.Term:%v, args.LastIncludedIndex:%v", rf.me, args.LeaderId, args.Term, args.LastIncludedIndex)
		reply.Success = false
		return
	}

	if args.LastIncludedIndex <= rf.LastIncludedIndex || args.LastIncludedIndex <= rf.lastApplied {
		PrintfWarn("%v NoOp Snapshot from %v as log is update. args.Term:%v, args.LastIncludedIndex:%v", rf.me, args.LeaderId, args.Term, args.LastIncludedIndex)
		reply.Success = true
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.stepDown(args.Term, "detected stale term in InstallSnapshot")
	}

	// Update election timer if RPC has the same or greater term.
	rf.updateElectionTimer()

	reply.Success = true

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	// PrintfWarn("%v Installing Snapshot from %v. rf.commitIndex:%v, rf.lastApplied:%v", rf.me, args.LeaderId, rf.commitIndex, rf.lastApplied)
	if rf.LastIncludedIndex > 0 && len(args.Data) > 0 {
		// snapshot data invalid
		// send snapshot back to upper-level service
		applyMsg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: rf.LastIncludedIndex,
			SnapshotTerm:  rf.LastIncludedTerm,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied = args.LastIncludedIndex

		PrintfSuccess("%v:%v applied Snapshot. lastApplied: %v", rf.me, rf.state, rf.lastApplied)
	} else {
		PrintfWarn("%v not applying snapshot from %v. rf.LastIncludedIndex:%v, len(args.Data):%v", rf.me, args.LeaderId, rf.LastIncludedIndex, len(args.Data))
	}

	// 6. If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	for i := range rf.Log {
		if rf.Log[i].Index == args.LastIncludedIndex && rf.Log[i].Term == args.LastIncludedTerm {
			rf.Log = rf.Log[i+1:]
			PrintfDebug("%v Retain log after index: %d term: %d, remain %d logs", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.Log))
			return
		}
	}

	// 7. Discard the entire log
	rf.Log = []Log{}

	PrintfWarn("%v:%v:%v Installed Snapshot from %v. LI:{I:%v, T:%v}. rf.lastLogIndex:%v", rf.me, rf.state, rf.CurrentTerm, args.LeaderId, rf.LastIncludedIndex, rf.LastIncludedTerm, rf.lastLogIndex())

	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) createAndSendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	result := rf.sendInstallSnapshot(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !result {
		PrintfError("%v sendInstallSnapshot to %v timed-out!, %v, %v.", rf.me, server, args.toString(), &reply)
	} else {
		if reply.Success {
			rf.matchIndex[server] = rf.LastIncludedIndex
			rf.nextIndex[server] = rf.LastIncludedIndex + 1
			rf.commitLogs()
			rf.persist()
			PrintfSuccess("%v sendInstallSnapshot to %v success!, %v, %v. rf.nextIndex[server]:%v", rf.me, server, args.toString(), &reply, rf.nextIndex[server])
		} else {
			PrintfWarn("%v sendInstallSnapshot to %v failed!, %v, %v.", rf.me, server, args.toString(), &reply)
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// return false if its not the leader
	if !rf.isleader() {
		// PrintfInfo("%v:%v Start(%v). result: false", rf.me, rf.state, command)
		return 0, rf.CurrentTerm, false
	}

	// Step 1: append entry to local log as a new entry,
	// index at which log entry will be written.
	PrintfSuccess("%v rf.nextIndex(%v)", rf.me, rf.nextIndex)
	newLogIndex := rf.nextIndex[rf.me]
	newLog := Log{
		Command: command,
		Term:    rf.CurrentTerm,
		Index:   newLogIndex,
	}

	// if len(rf.Log) >= 1 && rf.Log[len(rf.Log)-1].Command == command {
	// 	panic(fmt.Sprintf("%v trying to start duplicate Log %v in %v", rf.me, command, rf.Log))
	// }

	rf.Log = append(rf.Log, newLog)
	rf.matchIndex[rf.me]++
	rf.nextIndex[rf.me]++
	rf.newLogEntries = true
	PrintfSuccess("%v:%v Start(%v). result: true. newLogIndex: %v", rf.me, rf.state, command, newLogIndex)
	PrintfPurple("%v:%v log: %v. commitIndex: %v. lastApplied: %v", rf.me, rf.state, rf.Log, rf.commitIndex, rf.lastApplied)

	// Step 2: (This will happen in rf.sendAppendEntriesToAllPeers() in the next trigger loop)
	// issue AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
	// If followers crash or run slowly, or if network packets are lost,
	// the leader retries AppendEntries RPCs indefinitely (even after it has responded to the client)
	// until all followers eventually store all log entries.
	rf.updateHeartBeatTimer(true)

	rf.persist()

	return newLogIndex, rf.CurrentTerm, true
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

		// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate:
		// convert to candidate
		rf.mu.Lock()
		// PrintfDebug("%v ticker: %v. mutex:%v", rf.me, rf.state, reflect.ValueOf(&rf.mu).Elem().FieldByName("state"))
		if rf.state == Follower || rf.state == Candidate {
			// PrintfDebug("%v Follower election timer: %v", rf.me, time.Since(rf.electionTimer))
			if rf.electionTimer.Before(time.Now()) {
				PrintfWarn("%v term %v election timer expired, will attempt election soon...timer:%v , now:%v", rf.me, rf.CurrentTerm, rf.electionTimer, time.Now())
				rf.state = Candidate
				// Update election timer once peer becomes a candidate
				rf.updateElectionTimer()

				go rf.attemptElection(rf.CurrentTerm+1, 0)
			}
		} else if rf.state == Leader {
			if rf.heartbeatTimer.Before(time.Now()) {
				// Update election timer before sending heartbeat
				// PrintfInfo("%v sendAppendEntriesToAllPeers: %v", rf.me, time.Since(rf.heartbeatTimer))
				go rf.sendAppendEntriesToAllPeers()
			}
			if rf.electionTimer.Before(time.Now()) {
				// downgrade leadership
				// PrintfWarn("%v term %v election timer expired, stepping down. timer:%v , now:%v", rf.me, rf.CurrentTerm, rf.electionTimer, time.Now())
				rf.stepDown(rf.CurrentTerm, "electionTimer has expired")
			}
		}
		rf.persist()

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) attemptElection(term int, retryCount int) {
	if retryCount > 20 {
		panic(fmt.Sprintf("%v attemptElection retryCount > %v", rf.me, retryCount))
	}
	// sleep for random time
	// ms := (rand.Int() % 70)
	// PrintfInfo("%v sleeping for %v before election", rf.me, ms)
	// time.Sleep(time.Duration(ms) * time.Millisecond)
	rf.mu.Lock()
	currentState := rf.state
	rf.mu.Unlock()
	if currentState != Candidate {
		// PrintfWarn("%v Skipping election as state has changed to %v", rf.me, currentState)
		return
	}

	PrintfInfo("%v attempting election for term: %v. retryCount: %v", rf.me, term, retryCount)

	// On conversion to candidate, start election.
	rf.mu.Lock()
	rf.CurrentTerm = term
	rf.VotedFor = rf.me
	votesGranted := 1
	votesTotal := 1
	rf.persist()
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int, mu *sync.Mutex) {
			mu.Lock()
			request := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm(),
			}
			mu.Unlock()

			var reply RequestVoteReply
			// PrintfInfo("%v b4.sendRequestVote for term %v to %v.", rf.me, request.Term, server)
			result := rf.sendRequestVote(server, &request, &reply)

			mu.Lock()
			votesTotal += 1
			if !result {
				PrintfError("%v sendRequestVote request to %v timed-out!", rf.me, server)
			} else {
				if reply.VoteGranted {
					votesGranted += 1
					PrintfSuccess("%v was granted vote from %v. term %v. votesGranted: %v", rf.me, server, request.Term, votesGranted)
				} else {
					PrintfWarn("%v was not granted vote from %v. term %v. votesGranted: %v", rf.me, server, request.Term, votesGranted)
				}
			}
			cond.Broadcast()
			mu.Unlock()

		}(i, &rf.mu)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// wait till we get all votes or get enough upvotes
	// NOTE: this loop needs to wait for either (votesGranted or votesTotal)
	for votesGranted < rf.quorum && votesTotal < len(rf.peers) {
		cond.Wait()
		if rf.state != Candidate || rf.CurrentTerm != term {
			PrintfWarn("%v ignoring voting result as state or term has changed", rf.me)
			return
		}
	}

	// If votes received from majority of servers: become leader
	// else, retry election
	if votesGranted >= rf.quorum {
		rf.state = Leader

		// When a leader first comes to power,
		// it initializes all nextIndex values to the index just after the last one in its log
		for i := 0; i < len(rf.peers); i++ {
			// rf.matchIndex[i] = rf.lastLogIndex()
			rf.nextIndex[i] = rf.lastLogIndex() + 1
		}

		PrintfSuccess("------- %v won election for term %v -------", rf.me, rf.CurrentTerm)
		PrintfPurple("%v:%v log: %v. commitIndex: %v, lastApplied:%v", rf.me, rf.state, rf.Log, rf.commitIndex, rf.lastApplied)

		// will trigger append entries
		rf.updateHeartBeatTimer(true)
		rf.updateElectionTimer()
		// go rf.sendAppendEntriesToAllPeers()
	} else {
		PrintfError("------- %v lost election for term %v -------", rf.me, rf.CurrentTerm)
		rf.updateElectionTimer()
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

	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.initElectionTimer()
	rf.state = Follower // start off as follower
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
