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
	"sort"
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
		votes: make(map[uint64]bool),
		Prs: make(map[uint64]*Progress),
	}
	lastLogIndex := res.RaftLog.LastIndex()
	for _, id := range c.peers {
		if id == res.id {
			res.Prs[id] = &Progress{Next: lastLogIndex+1, Match: lastLogIndex}
		} else {
			res.Prs[id] = &Progress{Next: lastLogIndex+1}
		}
	}
	res.becomeFollower(0, None)
	res.resetRandomElectionTimeout()
	// 从 log 中恢复状态信息
	state, _, _ := res.RaftLog.storage.InitialState()
	res.Term, res.Vote, res.RaftLog.committed = state.GetTerm(), state.GetVote(), state.GetCommit()
	if c.Applied > 0 {
		res.RaftLog.applied = c.Applied
	}
	return res
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
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
		if err != nil {
			log.Printf(r.nodeInfo() + err.Error())
		}
	}
}

// tickElection provide a method that leader can send heartbeat to followers
func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatElapsed {
		r.heartbeatElapsed = 0
		log.Printf(r.nodeInfo() + "heartbeat timeout\n")
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
		if err != nil {
			log.Printf(r.nodeInfo() + err.Error())
		}
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
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	lastLogIndex := r.RaftLog.LastIndex()
	r.heartbeatElapsed = 0

	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = lastLogIndex + 2
			r.Prs[peer].Match = lastLogIndex + 1
		} else {
			r.Prs[peer].Next = lastLogIndex + 1
		}
	}
	//log.Printf(r.nodeInfo() + "become a leader\n")
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex()+1})
	r.broadcastAppendEntries()
	if len(r.Prs) == 1 {
		// single server
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		// in this case, server need to update its term and become a follower
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		return r.followerStep(m)
	case StateCandidate:
		return r.candidateStep(m)
	case StateLeader:
		return r.leaderStep(m)
	}
	return nil
}

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
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleSnapshot(m)
	}
	return nil
}

func (r *Raft) leaderStep(m pb.Message) error {
	//log.SetPrefix("[leaderStep()]")
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			r.appendEntries(m.Entries)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	}
	return nil
}

func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.broadcastAppendEntries()
		}
	}
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	if len(r.Prs) == 1 {
		// 集群节点数=1 时，直接当选
		r.becomeLeader()
		return
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer, lastLogIndex, lastLogTerm)
	}
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, term, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		LogTerm: term,
		Index:   index,
	}
	r.sendMessage(msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// TODO: 内部逻辑太多，考虑重构
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	// 重置选举超时
	r.resetRandomElectionTimeout()
	r.Lead = m.From
	log := r.RaftLog
	lastLogIndex := log.LastIndex()

	if m.Index > lastLogIndex {
		r.sendAppendResponse(m.From, true, None, lastLogIndex+1)
		return
	}

	if m.Index >= log.FirstIndex {
		logTerm, err := log.Term(m.Index)
		if err != nil {
			panic(err)
		}
		if logTerm != m.LogTerm {
			index := log.entryIndex(sort.Search(log.sliceIndex(m.Index+1),
				func(i int) bool { return log.entries[i].Term == logTerm }))
			reject := true
			r.sendAppendResponse(m.From, reject, logTerm, index)
			return
		}
	}

	for i, entry := range m.Entries {
		if entry.Index < log.FirstIndex {
			// 早于当前最早的 index 记录
			continue
		}
		if entry.Index <= log.LastIndex() {
			logTerm, err := log.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != entry.Term {
				// 用接收到的 entry 覆盖之前的 entry
				idx := log.sliceIndex(entry.Index)
				log.entries[idx] = *entry
				// TODO: 为什么要做这一步
				log.entries = log.entries[:idx+1]
				// Term 不匹配，由此开始的 entry 都不是 stabled
				log.stabled = min(log.stabled, entry.Index-1)
			}
		} else {
			// [i, n) 区间的 entry 都是新记录，直接 append
			n := len(m.Entries)
			for j := i; j < n; j++ {
				log.entries = append(log.entries, *m.Entries[j])
			}
			break
		}
	}
	if m.Commit > log.committed {
		log.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	reject := false
	r.sendAppendResponse(m.From, reject, None, log.LastIndex())
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastLogIndex := r.RaftLog.LastIndex()
	for i, ent := range entries {
		ent.Term = r.Term
		ent.Index = lastLogIndex + uint64(i) + 1
		// TODO: Config 相关功能未实现
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.broadcastAppendEntries()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && r.Term > m.Term {
		return
	}
	if m.Reject {
		index := m.Index
		if index == None {
			return
		}
		if m.LogTerm != None {
			log := r.RaftLog
			logTerm := m.LogTerm
			sliceIndex := sort.Search(len(log.entries), func(i int) bool {
				return log.entries[i].Term > logTerm
			})
			if sliceIndex > 0 && log.entries[sliceIndex-1].Term == logTerm {
				index = log.entryIndex(sliceIndex)
			}
		}
		// 重新从 index 开始发送 entry
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index+1
		r.leaderCommit()
		// TODO: leaderTransfer
	}

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
	if m.Term != None &&  m.Term < r.Term {
		resp.Reject = true
	}
	r.Lead = m.From
	r.resetRandomElectionTimeout()
	r.sendMessage(resp)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	r.sendAppend(m.From)
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
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	if lastLogTerm > m.LogTerm ||
		lastLogTerm == m.LogTerm && lastIndex > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject

	yea := 0
	no := 0
	threshold := len(r.Prs) / 2
	for _, vote := range r.votes {
		if vote == true {
			yea++
		} else {
			no++
		}
	}
	if yea > threshold {
		r.becomeLeader()
	}
	if no > threshold {
		r.becomeFollower(m.Term, None)
	}
}

// sendMessage append RPC message to r.msgs
func (r *Raft) sendMessage(m pb.Message) {
	r.msgs = append(r.msgs, m)
	//if m.MsgType == pb.MessageType_MsgAppendResponse {
	//	log.Printf(r.nodeInfo() + "send a append response %+v to node %d\n", m, m.To)
	//}
	//if m.MsgType == pb.MessageType_MsgAppend {
	//	for _, entry := range m.Entries {
	//		log.Printf(r.nodeInfo() + "send append entry[%d, %d, Commit = %d] to %d\n",
	//			entry.Index, entry.Term, m.Commit, m.To)
	//	}
	//}
}

// sendRequestVote candidate send RequestVote message to a peer
func (r *Raft) sendRequestVote(to, logIndex, logTerm uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: logTerm,
		Index: logIndex,
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

func (r *Raft) broadcastAppendEntries() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next-1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		panic(err)
	}

	var entries []*pb.Entry
	n := len(r.RaftLog.entries)

	for i := r.RaftLog.sliceIndex(prevLogIndex+1); i < n; i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From: r.id,
		To: to,
		Term: r.Term,
		Commit: r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index: prevLogIndex,
		Entries: entries,
	}

	r.sendMessage(msg)
	return true
}

func (r *Raft) resetRandomElectionTimeout() {
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) broadcastHeartbeat() {
	//log.SetPrefix("[broadcastHeartbeat()]")
	if r.State != StateLeader {
		log.Panicf(r.nodeInfo() + "is not a leader but try to broadcast heartbeat\n")
	}
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
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

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term: r.Term,
		Vote: r.Vote,
		Commit: r.RaftLog.committed,
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