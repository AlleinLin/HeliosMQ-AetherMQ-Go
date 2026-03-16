package raft

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type RaftNode struct {
	nodeID     string
	address    string
	state      RaftState
	leaderID   string
	term       uint64
	commitIdx  uint64
	lastApplied uint64

	log        []*LogEntry
	appliedCh  chan *LogEntry

	mu         sync.RWMutex
	logger     *zap.Logger
	running    bool
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index uint64
	Term  uint64
	Cmd   *Command
}

type Command struct {
	Op    OpType
	Key   string
	Value interface{}
}

type OpType int

const (
	CreateTopic OpType = iota
	DeleteTopic
	RegisterBroker
	UnregisterBroker
	UpdatePartition
	CommitOffset
)

func NewRaftNode(ctx context.Context, nodeID, address, dataDir, joinAddr string) (*RaftNode, error) {
	node := &RaftNode{
		nodeID:    nodeID,
		address:   address,
		state:     Follower,
		log:       make([]*LogEntry, 0),
		appliedCh: make(chan *LogEntry, 1000),
		logger:    zap.NewNop(),
	}

	go node.run(ctx)

	return node, nil
}

func (n *RaftNode) run(ctx context.Context) {
	n.running = true
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for n.running {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			switch n.state {
			case Follower:
				n.followerTick()
			case Candidate:
				n.candidateTick()
			case Leader:
				n.leaderTick()
			}
			n.mu.Unlock()
		}
	}
}

func (n *RaftNode) followerTick() {
}

func (n *RaftNode) candidateTick() {
	n.state = Leader
	n.leaderID = n.nodeID
	n.term++
	n.logger.Info("became leader", zap.Uint64("term", n.term))
}

func (n *RaftNode) leaderTick() {
}

func (n *RaftNode) Propose(ctx context.Context, cmd *Command) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return ErrNotLeader
	}

	entry := &LogEntry{
		Index: uint64(len(n.log)),
		Term:  n.term,
		Cmd:   cmd,
	}
	n.log = append(n.log, entry)
	n.commitIdx = entry.Index
	n.lastApplied = entry.Index

	select {
	case n.appliedCh <- entry:
	default:
	}

	return nil
}

func (n *RaftNode) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == Leader
}

func (n *RaftNode) GetLeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *RaftNode) GetTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

func (n *RaftNode) Close() {
	n.running = false
	close(n.appliedCh)
}

var ErrNotLeader = &RaftError{Message: "not the leader"}

type RaftError struct {
	Message string
}

func (e *RaftError) Error() string {
	return e.Message
}
