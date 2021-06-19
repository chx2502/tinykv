// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// a random interval between [electionTimeout, 2 * electionTimeout - 1]
	randomElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	res := &Raft{
		id: c.ID,
		RaftLog: newLog(c.Storage),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		Lead: None,
		votes: make(map[uint64]bool),
		Prs: make(map[uint64]*Progress),
	}
	lastIndex := res.RaftLog.LastIndex()
	for _, id := range c.peers {
		res.votes[id] = false
		if id == res.id {
			res.Prs[id] = &Progress{Next: lastIndex+1, Match: lastIndex}
		} else {
			res.Prs[id] = &Progress{Next: lastIndex+1}
		}
	}
	res.becomeFollower(0, None)
	res.randomElectionTimeout = res.electionTimeout + rand.Intn(res.electionTimeout)
	return res
}
// sendMessage append RPC message to r.msgs
func (r *Raft) sendMessage(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// sendRequestVote candidate send RequestVote message to a peer
func (r *Raft) sendRequestVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: lastLogTerm,
	}
	r.sendMessage(m)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Reject: reject,
	}
	r.sendMessage(m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

func (r *Raft) broadcastHeartbeat() {
	//log.SetPrefix("[broadcastHeartbeat()]")
	if r.State != StateLeader {
		log.Panicf(r.nodeInfo() + "is not a leader but try to broadcast heartbeat\n")
	}
	for i := 1; i <= len(r.Prs); i++ {
		if uint64(i) == r.id {
			continue
		}
		r.sendHeartbeat(uint64(i))
	}
	//log.Printf(r.nodeInfo() + "broadcasts heartbeat to all peers\n")
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
	}
	r.sendMessage(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//log.SetPrefix("[tick()]")
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartBeat()
	}
	//log.Println(r.nodeInfo() + "tick\n")
}

// tickElection update the electionElapsed
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// send message `MessageType_MsgHup` to self
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
	}
}

// tickElection provide a method that leader can send heartbeat to followers
func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatElapsed {
		r.heartbeatElapsed = 0
		log.Printf(r.nodeInfo() + "heartbeat timeout\n")
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
	}
}


// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Vote = None
	r.Term = term
	//log.Printf(r.nodeInfo() + "become a follower\n")
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	//r.refresh(r.Term+1)
	r.State = StateCandidate
	r.Lead = None
	r.Vote = r.id
	r.Term = r.Term + 1
	r.votes[r.id] = true

	// update votes
	for id := range r.votes {
		if id == r.id {
			r.votes[id] = true
		} else {
			r.votes[id] = false
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()

	for id := range r.Prs {
		if id == r.id {
			r.Prs[id] = &Progress{Next: lastIndex+1, Match: lastIndex}
		} else {
			r.Prs[id] = &Progress{Next: lastIndex+1}
		}
	}
	//log.Printf(r.nodeInfo() + "become a leader\n")
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		//log.Printf(
		//	"<Node %d (Term=%d)> received %v meesage " +
		//		"from <Node %d (Term=%d)>\n",
		//	r.id, r.Term, m.MsgType, m.From, m.Term,
		//)
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	}

	switch r.State {
	case StateFollower:
		r.followerStep(m)
	case StateCandidate:
		r.candidateStep(m)
	case StateLeader:
		r.leaderStep(m)
	}
	return nil
}

// TODO: (r *Raft) followerStep(m pb.Message) error {}
// TODO: func (r *Raft) candidateStep(m pb.Message) error {}
// TODO: func (r *Raft) leaderStep(m pb.Message) error {}

func (r *Raft) followerStep(m pb.Message) error {
	//log.SetPrefix("[followerStep()]")
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

func (r *Raft) candidateStep(m pb.Message) error {
	//log.SetPrefix("[candidateStep()]")
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

func (r *Raft) leaderStep(m pb.Message) {
	//log.SetPrefix("[leaderStep()]")
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgRequestVote:
		r.sendMessage(
			pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From: r.id,
				To: m.From,
				Reject: true,
			})
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	}
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	if len(r.votes) == 1 {
		// if there is only one server, current server win
		r.becomeLeader()
	}
	for id := range r.votes {
		if id == r.id {
			continue
		} else {
			r.sendRequestVote(id)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//log.Printf(r.nodeInfo() + "received heartbeat from Node %d (Term=%d)\n", m.From, m.Term)

	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Reject: false,
	}
	if m.Term < r.Term {
		resp.Reject = true
	}
	r.sendMessage(resp)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	return
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleHup(m pb.Message) {
	r.campaign()
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := false
	if r.Term > m.Term {
		reject = true
		r.sendRequestVoteResponse(m.From, reject)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		reject = true
		r.sendRequestVoteResponse(m.From, reject)
		return
	}
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}
	if lastLogTerm > m.LogTerm || (lastLogTerm == m.LogTerm && lastLogIndex > m.Index) {
		reject = true
		r.sendRequestVoteResponse(m.From, reject)
		return
	}
	r.Vote = m.From
	r.sendRequestVoteResponse(m.From, reject)
	return
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	yea := 0
	for _, vote := range r.votes {
		if vote == true {
			yea++
		}
	}
	if yea > len(r.votes) / 2 {
		r.becomeLeader()
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) nodeInfo() string {
	return fmt.Sprintf("<Node %d (Term=%d)> ", r.id, r.Term)
}