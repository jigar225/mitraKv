package raft

// State captures the role of a node in the Raft cluster.
type State int

const (
	// Follower replicas client reads and accepts leader heartbeats by default.
	Follower State = iota
	// Candidate requests votes when it suspects the leader is gone.
	Candidate
	// Leader is the only node allowed to accept client writes in Phase 3.
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Op identifies a state-machine command replicated through the Raft log.
type Op string

const (
	OpSet Op = "SET"
	OpDel Op = "DEL"
)

// Command is the payload applied to the KV store once an entry is committed.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// LogEntry is one durable Raft log record. The index is 1-based to match Raft papers.
type LogEntry struct {
	Index   uint64  `json:"index"`
	Term    uint64  `json:"term"`
	Command Command `json:"command"`
}
