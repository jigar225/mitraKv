package raft

// ApplyFunc applies a committed Raft command to the local state machine (KV store).
type ApplyFunc func(cmd Command)

// SetApplier registers how this node executes committed log entries on the store.
func (n *Node) SetApplier(fn ApplyFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applier = fn
}

// CommitIndex returns the highest log index known to be committed on this node.
func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LastApplied returns the highest log index applied to the state machine.
func (n *Node) LastApplied() uint64 {
	n.mu.RLock()
	defer n.mu.Unlock()
	return n.lastApplied
}
