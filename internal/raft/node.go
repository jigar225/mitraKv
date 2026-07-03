package raft

import (
	"sync"
	"time"
)

// Config holds node identity and timing knobs used by the Raft state machine.
type Config struct {
	NodeID             string
	Peers              []string
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// Node is a single Raft peer. It owns persistent log state and role transitions.
type Node struct {
	mu sync.RWMutex

	cfg Config

	state       State
	currentTerm uint64
	votedFor    string
	leaderID    string

	log *Log

	commitIndex uint64
	lastApplied uint64

	transport     Transport
	electionReset chan struct{}
	applier       ApplyFunc

	// leaderState tracks per-follower replication progress (leader only).
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
}

// NewNode constructs a follower node ready to join a cluster.
func NewNode(cfg Config) *Node {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax == 0 {
		cfg.ElectionTimeoutMax = 300 * time.Millisecond
	}

	return &Node{
		cfg:           cfg,
		state:         Follower,
		log:           NewLog(),
		electionReset: make(chan struct{}, 1),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
	}
}

// ID returns this node's unique identifier in the cluster.
func (n *Node) ID() string {
	return n.cfg.NodeID
}

// State returns the current role (follower, candidate, or leader).
func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// CurrentTerm returns the latest term this node has observed.
func (n *Node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// Log returns a snapshot reference to the underlying replicated log.
func (n *Node) Log() *Log {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.log
}

// becomeFollower resets election state when a node accepts a newer leader term.
func (n *Node) becomeFollower(term uint64) {
	n.state = Follower
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
}

// becomeCandidate increments the term and votes for itself to start an election.
func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.NodeID
}

// becomeLeader initializes replication tracking maps for each peer.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.nextIndex = make(map[string]uint64, len(n.cfg.Peers))
	n.matchIndex = make(map[string]uint64, len(n.cfg.Peers))

	next := n.log.LastIndex() + 1
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.NodeID {
			continue
		}
		n.nextIndex[peer] = next
		n.matchIndex[peer] = 0
	}
}

// IsLeader reports whether this node currently owns client write responsibility.
func (n *Node) IsLeader() bool {
	return n.State() == Leader
}
