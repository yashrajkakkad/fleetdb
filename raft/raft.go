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

	// Other variables
	lastHeard time.Time
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

	// channels for communication
	// sent by the leader to rest of the servers
	heartBeat chan bool
	// send to candidate if we win election i.e received more than half of the votes
	winner chan bool
	// election timeout: if this is true, there exists no leader, turn the follower->candidate
	electionTimout chan bool
}

type Log struct {
	// Blank for now, until we implement log replication
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// do we need locks here?
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
	Term        int
	CandidateId int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Should this be done only if a server is a follower?
	rf.mu.Lock()
	// if candidate is behind, do not grant vote
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // -1 is null for now
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId // give vote
		DPrintf("%d became a follower by giving vote to %d", rf.me, rf.votedFor)
		rf.state = "Follower" // change state to follower (what if a candidate receives request vote rpc?)
	} else {
		reply.VoteGranted = false
	}

	if args.Term > rf.currentTerm { // Convert to follower
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%d received heartbeat from %d", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeard = time.Now()
	if rf.state == "Leader" {
		if args.Term > rf.currentTerm { // Convert to follower
			DPrintf("%d is a follower as it discovered a new leader %d with a newer term", rf.me, args.LeaderId)
			rf.state = "Follower"
			rf.currentTerm = args.Term
		}
		return
	}
	if rf.state == "Candidate" {
		rf.heartBeat <- true
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
		if args.Term > rf.currentTerm { // Convert to follower
			rf.currentTerm = args.Term
			rf.state = "Follower"
		}
	}
	reply.Term = rf.currentTerm
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
		}
		// if we receive major votes, --> leader
		if reply.VoteGranted {
			rf.voteRecieved += 1
			if rf.state == "Candidate" && rf.voteRecieved*2 > len(rf.peers) {
				rf.state = "Leader"
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

	rf.heartBeat = make(chan bool)
	rf.winner = make(chan bool)
	rf.electionTimout = make(chan bool)

	// routine which checks the election timeout (implementing this as an anonymous routine now)
	go func() {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(200)+500))
		rf.mu.Lock()
		rf.electionTimout <- true
		rf.mu.Unlock()
	}()
	// start the raft algorithm
	go rf.Run()

	// go func() {
	// 	time.Sleep(150 * time.Millisecond)
	// 	if rf.state == "Leader" {
	// 		for j := 0; j < len(rf.peers); j++ {
	// 			if j == me {
	// 				continue
	// 			}
	// 			heartbeatArgs := &AppendEntriesArgs{rf.currentTerm, me}
	// 			go func(j int) {
	// 				heartbeatReply := &AppendEntriesReply{}
	// 				peers[j].Call("Raft.AppendEntries", heartbeatArgs, heartbeatReply)
	// 			}(j)
	// 		}
	// 	}
	// }()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf

}

func (rf *Raft) SendAppendEntries() {
	// TODO: Tomorrow
	for j := 0; j < len(rf.peers); j++ {
		if j == rf.me {
			continue
		}
		heartbeatArgs := &AppendEntriesArgs{rf.currentTerm, rf.me}
		go func(j int) {
			heartbeatReply := &AppendEntriesReply{}
			rf.peers[j].Call("Raft.AppendEntries", heartbeatArgs, heartbeatReply)
		}(j)
	}
}

// sendVOteRequests is supposed to send vote requests to all the servers in the cluster
func (rf *Raft) sendVoteRequests() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	me := rf.me
	rf.mu.Unlock()
	for i, r := range rf.peers {
		// don't send to self!
		if rf.peers[me] == r {
			continue
		}
		// send RPC in parallel (Why tf does this give me data race?)
		var reply RequestVoteReply
		go rf.sendRequestVote(i, &args, &reply)
	}
}

func (rf *Raft) Run() {
	for {
		rf.mu.Lock()
		currentState := rf.state
		rf.mu.Unlock()

		if currentState == "Leader" {
			/* if leader, send append entries every 120 seconds? */
			DPrintf("%d sending heartbeats to continue claiming leadership for term %d", rf.me, rf.currentTerm)
			go rf.SendAppendEntries() // this should just send an heartbeat
			time.Sleep(time.Millisecond * 120)
		}

		if currentState == "Follower" {
			DPrintf("%d is a follower", rf.me)
			select {
			// do nothing if receive heartbeat, all is fine
			case <-rf.heartBeat:
			case <-rf.electionTimout:
				rf.mu.Lock()
				DPrintf("%d became candidate for election", rf.me)
				rf.state = "Candidate"
				rf.mu.Unlock()
			}
			// electionTimeout := time.Duration(rand.Intn(max-min)+min) * time.Millisecond
			// if time.Now().Sub(rf.lastHeard) < electionTimeout {
			// 	time.Sleep(electionTimeout - (time.Now().Sub(rf.lastHeard)))
			// }
		}

		if currentState == "Candidate" {
			// a candidate would vote for self and then send requestVoteRPC to other servers (figure 2-Rule for servers-Candidates)
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.voteRecieved = 1
			rf.currentTerm++
			rf.mu.Unlock()
			// i think that this routine should send notification (channel) state the result of the election.
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
				DPrintf("%d became follower from candidate as it received a heartbeat", rf.me)
				rf.state = "Follower"
				rf.mu.Unlock()
			// if we win the election
			case <-rf.winner:
				rf.mu.Lock()
				DPrintf("%d won the election and became a leader", rf.me)
				rf.state = "Leader"
				rf.mu.Unlock()
			case <-rf.electionTimout:
				rf.mu.Lock()
				DPrintf("%d Election timed out", rf.me)
				rf.mu.Unlock()
			}
		}

		// The server requests for votes
		// args := &RequestVoteArgs{rf.currentTerm, rf.me}
		// votes := 0
		// for i := 0; i < len(rf.peers); i++ {
		// 	if i == rf.me {
		// 		continue
		// 	}
		// 	// delegating to other func
		// 	go func(i int) { // Send votes in parallel
		// 		reply := &RequestVoteReply{}
		// 		rf.peers[i].Call("Raft.RequestVote", args, reply) // Do we have to repeat the call if it is false?
		// 		if reply.VoteGranted == true {
		// 			votes++ // Does this have to be locked?
		// 			if votes > len(rf.peers)/2 {
		// 				rf.state = "leader"
		// 				for j := 0; j < len(rf.peers); j++ {
		// 					if j == rf.me {
		// 						continue
		// 					}
		// 					heartbeatArgs := &AppendEntriesArgs{rf.currentTerm, rf.me}
		// 					go func(j int) {
		// 						heartbeatReply := &AppendEntriesReply{}
		// 						rf.peers[j].Call("Raft.AppendEntries", heartbeatArgs, heartbeatReply)
		// 					}(j)
		// 				}
		// 			}
		// 		}
		// 	}(i)
		// }
	}
}
