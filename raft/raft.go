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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yashrajkakkad/fleetdb/labrpc"
)

// import "bytes"
// import "github.com/yashrajkakkad/fleetdb/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []*Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	/*
		   state is the current state of the server
		   at a single time, a server can  be:
			Follower
		    Candidate
		   	Leader
	*/
	state string
	// total voteRecieved by the candidate, if more than half --> leader
	voteRecieved int

	applyCh chan ApplyMsg

	// channels for communication
	// sent by the leader to rest of the servers
	heartBeat chan bool
	// send to candidate if we win election i.e received more than half of the votes
	winner chan bool
}

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == "Leader"
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// From the paper: Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
func (rf *Raft) isLogUptodate(lastIndex int, lastTerm int) bool {
	// rf.mu.Lock()
	currLastIndex := len(rf.log) - 1
	DPrintf("currLastIndex: %d\n", currLastIndex)
	currLastTerm := rf.log[currLastIndex].Term
	// rf.mu.Unlock()

	if currLastTerm == lastTerm {
		return lastIndex >= currLastIndex
	}
	return lastTerm > currLastTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUptodate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictingIndex = len(rf.log)
		return
	}

	// send heartbeat
	rf.heartBeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
	}

	reply.ConflictingIndex = -1
	reply.Term = rf.currentTerm

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// but first we have to check if follower log length is less than leader logIndex (to further index out of bound)
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Success = false
		reply.ConflictingIndex = len(rf.log)
		return
	}

	matchTerm := rf.log[args.PrevLogIndex].Term
	// log doesn't contain an entry at prevLogIndex. Inconsistency. Back off
	if args.PrevLogIndex > 0 && matchTerm != args.PrevLogTerm {
		for i := args.PrevLogIndex; i >= 0 && rf.log[i].Term == matchTerm; i-- {
			reply.ConflictingIndex = i
		}
		return
	} else {
		rf.log = rf.log[:args.PrevLogIndex+1]
		currLog := rf.log[args.PrevLogIndex+1:]

		if rf.isLogConflicted(args.Entries, currLog) || len(currLog) < len(args.Entries) {
			rf.log = append(rf.log, args.Entries...)
		} else {
			rf.log = append(rf.log, currLog...)
		}
		reply.Success = true
		reply.ConflictingIndex = args.PrevLogIndex

		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
			go rf.applyLog()
		}
	}
}

func (rf *Raft) isLogConflicted(leaderLog []*Log, peerLog []*Log) bool {
	for i := 0; i < len(leaderLog) && i < len(peerLog); i++ {
		if leaderLog[i].Term != peerLog[i].Term {
			return true
		}
	}
	return false
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.log[i].Command
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) SendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	peer := rf.peers[server]
	rf.mu.Unlock()
	ok := peer.Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = "Follower"
			rf.votedFor = -1
			return ok
		}
	}
	return ok
}

func (rf *Raft) SendAppendEntries(i int) {
	DPrintf("Inside SendAppendEntries")
	for {
		rf.mu.Lock()
		if rf.state != "Leader" {
			DPrintf("Returning")
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// DPrintf("Sending AppendEntries for log replication")
		// DPrintf("Lock acquired by SendAppendEntries")
		rf.mu.Lock()
		lastLogIndex := len(rf.log) - 1
		sendRPC := false
		if lastLogIndex >= rf.nextIndex[i] {
			sendRPC = true
		}
		rf.mu.Unlock()
		if sendRPC {
			var args AppendEntriesArgs
			var reply AppendEntriesReply
			// DPrintf("Lock acquired by SendAppendEntries to send RPC")
			rf.mu.Lock()
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex >= 0 {
				args.Term = rf.log[args.PrevLogIndex].Term
			} else {
				args.Term = 0
			}
			args.Entries = rf.log[rf.nextIndex[i]:]
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			ok := rf.SendAppendEntriesRPC(i, &args, &reply)
			DPrintf("Response received from AppendEntries: %v", ok)
			if ok {
				// DPrintf("Lock acquired by SendAppendEntriesRPC ok =true")
				rf.mu.Lock()
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = lastLogIndex + 1
				rf.mu.Unlock()
			} else {
				// DPrintf("Lock acquired by SendAppendEntriesRPC ok =false")
				rf.mu.Lock()
				rf.nextIndex[i]--
				rf.mu.Unlock()
			}
		} else {
			time.Sleep(10 * time.Millisecond)

		}

	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// if the currentTerm is less than other server's term, update term and become follower
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = "Follower"
			rf.votedFor = -1
			return ok
		}
		// if we receive major votes, --> leader
		if reply.VoteGranted {
			rf.voteRecieved += 1
			if rf.state == "Candidate" && rf.voteRecieved*2 > len(rf.peers) {
				rf.state = "Leader"
				DPrintf("%d became leader", rf.me)
				rf.becameLeader()
				// send channel response
				rf.winner <- true
			}
		}
	}
	return ok
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
	// DPrintf("Start triggered")
	// DPrintf("Lock acquired by Start")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.state == "Leader")

	// Your code here (2B).
	if !isLeader {
		DPrintf("Exiting from Start")
		return index, term, isLeader
	}

	// append to the leader log
	log := &Log{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, log)
	DPrintf("Exiting from Start as leader")
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// all servers start in the follower state
	rf.state = "Follower"
	// term is 0 at startup
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteRecieved = 0

	rf.applyCh = applyCh
	rf.heartBeat = make(chan bool, 10)
	rf.winner = make(chan bool, 10)
	rf.log = append(rf.log, &Log{Term: 0})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// start the raft algorithm
	go rf.Run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf

}

func (rf *Raft) SendAppendEntriestoAll() {
	var args AppendEntriesArgs
	var reply AppendEntriesReply
	// DPrintf("Lock acquired by SendAppendEntriestoAll")
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	for i, r := range rf.peers {
		if peers[me] == r {
			continue
		}
		replyVar := reply
		go rf.SendAppendEntriesRPC(i, &args, &replyVar)
	}
}

func (rf *Raft) becameLeader() {
	// Initialize next index and match index
	// DPrintf("Lock acquired by becameLeader()")
	// rf.mu.Lock()
	DPrintf("For loop to update nextIndex and matchIndex")
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	me := rf.me
	// rf.mu.Unlock()
	// Trigger goroutines for sending AppendEntries
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		go rf.SendAppendEntries(i)
	}
	go rf.updateCommitIndex()
}

// sendVoteRequests is supposed to send vote requests to all the servers in the cluster
func (rf *Raft) sendVoteRequests() {
	var args RequestVoteArgs
	// DPrintf("Lock acquired by sendVoteRequests()")
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.log) - 1
	DPrintf("length of the log: %d", len(rf.log))
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	me := rf.me
	rf.mu.Unlock()
	for i, r := range rf.peers {
		// don't send to self!
		if rf.peers[me] == r {
			continue
		}
		var reply RequestVoteReply
		go rf.sendRequestVote(i, &args, &reply)
	}
}

func (rf *Raft) updateLastApplied() {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	lastApplied := rf.lastApplied
	rf.mu.Unlock()
	if commitIndex > lastApplied {
		rf.mu.Lock()
		rf.lastApplied++
		rf.mu.Unlock()
		// Apply log to state machine?
	} else {
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	if rf.state != "Leader" {
		return
	}

	cond := true

	for cond {
		if rf.state != "Leader" {
			return
		}
		rf.mu.Lock()
		n := rf.commitIndex + 1
		DPrintf("Checking if %d can update commitIndex to %d", rf.me, n)
		if n >= len(rf.log) {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		cnt := 0
		for _, r := range rf.matchIndex {
			if r >= n {
				cnt++
			}
		}
		rf.mu.Lock()
		cond = cond && (cnt > (len(rf.peers) / 2))
		cond = cond && (rf.log[n].Term == rf.currentTerm)
		rf.mu.Unlock()
		if cond {
			rf.mu.Lock()
			rf.commitIndex++
			DPrintf("%d updated commitIndex to %d", rf.me, rf.commitIndex)
			rf.mu.Unlock()
		}
	}
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) Run() {
	for {
		// DPrintf("Lock acquired by Run()")
		rf.mu.Lock()
		currentState := rf.state
		rf.mu.Unlock()

		go rf.updateLastApplied()

		if currentState == "Leader" {
			go rf.SendAppendEntriestoAll() // this should just send an heartbeat or should it?
			time.Sleep(time.Millisecond * 100)
		}

		if currentState == "Follower" {
			//DPrintf("%d is a follower", rf.me)
			select {
			case <-rf.heartBeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				// DPrintf("Lock acquired by Run to change state to candidate")
				rf.mu.Lock()
				rf.state = "Candidate"
				rf.mu.Unlock()
			}
		}

		if currentState == "Candidate" {
			// a candidate would vote for self and then send requestVoteRPC to other servers (figure 2-Rule for servers-Candidates)
			// DPrintf("Lock acquired by Run() as Candidate")
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.voteRecieved = 1
			rf.currentTerm++
			rf.mu.Unlock()
			go rf.sendVoteRequests()
			/*
				a candidate can recive:
				1. heartbeat -> convert to follower (another server is elected as leader)
				2. winner -> convert to Leader (we received the majority of votes)
			*/
			select {
			// if we recieve heartbeat. change to follower
			case <-rf.heartBeat:
				rf.mu.Lock()
				//DPrintf("%d became follower from candidate as it received a heartbeat", rf.me)
				rf.state = "Follower"
				rf.mu.Unlock()
			// if we win the election
			case <-rf.winner:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+500)):
			}
		}
	}
}
