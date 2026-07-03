package raft

import (
	"log/slog"
	"time"
)

// Propose appends a command on the leader and blocks until it is committed on a majority.
func (n *Node) Propose(cmd Command) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	n.log.Append(n.currentTerm, cmd)
	targetIndex := n.log.LastIndex()
	n.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.replicateToPeers()

		n.mu.Lock()
		committed := n.commitIndex >= targetIndex
		n.mu.Unlock()
		if committed {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return ErrCommitTimeout
}

// replicateToPeers sends AppendEntries to every follower and advances commit index on success.
func (n *Node) replicateToPeers() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	term := n.currentTerm
	leaderID := n.cfg.NodeID
	transport := n.transport
	peers := append([]string(nil), n.cfg.Peers...)
	n.mu.Unlock()

	if transport == nil {
		return
	}

	n.sendAppendEntriesToPeers(peers, term, leaderID, transport)
	n.advanceCommitIndex()
	n.applyCommittedEntries()
	n.sendCommitIndexToPeers(peers, term, leaderID, transport)
	n.applyCommittedEntries()
}

func (n *Node) sendAppendEntriesToPeers(peers []string, term uint64, leaderID string, transport Transport) {
	for _, peer := range peers {
		if peer == leaderID {
			continue
		}

		n.mu.Lock()
		leaderCommit := n.commitIndex
		next := n.nextIndex[peer]
		prevIndex := next - 1
		var prevTerm uint64
		if prevIndex > 0 {
			if prev, ok := n.log.EntryAt(prevIndex); ok {
				prevTerm = prev.Term
			}
		}
		entries := n.log.EntriesFrom(next)
		n.mu.Unlock()

		resp, err := transport.AppendEntries(peer, AppendEntriesRequest{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		})
		if err != nil {
			slog.Warn("append entries failed", "peer", peer, "err", err)
			continue
		}

		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term)
			n.mu.Unlock()
			return
		}

		if resp.Success {
			if len(entries) > 0 {
				lastNew := entries[len(entries)-1].Index
				n.matchIndex[peer] = lastNew
				n.nextIndex[peer] = lastNew + 1
			}
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		n.mu.Unlock()
	}
}

// sendCommitIndexToPeers pushes the latest commit index so followers can apply entries.
func (n *Node) sendCommitIndexToPeers(peers []string, term uint64, leaderID string, transport Transport) {
	n.mu.Lock()
	leaderCommit := n.commitIndex
	lastIndex := n.log.LastIndex()
	var lastTerm uint64
	if lastIndex > 0 {
		if entry, ok := n.log.EntryAt(lastIndex); ok {
			lastTerm = entry.Term
		}
	}
	n.mu.Unlock()

	for _, peer := range peers {
		if peer == leaderID {
			continue
		}

		_, err := transport.AppendEntries(peer, AppendEntriesRequest{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: lastIndex,
			PrevLogTerm:  lastTerm,
			LeaderCommit: leaderCommit,
		})
		if err != nil {
			slog.Warn("commit sync failed", "peer", peer, "err", err)
		}
	}
}

// HandleAppendEntries processes leader heartbeats and log replication on followers.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()

	if req.Term < n.currentTerm {
		resp := AppendEntriesResponse{Term: n.currentTerm, Success: false}
		n.mu.Unlock()
		return resp
	}

	if req.Term >= n.currentTerm {
		n.becomeFollower(req.Term)
		n.leaderID = req.LeaderID
	}

	select {
	case n.electionReset <- struct{}{}:
	default:
	}

	if req.PrevLogIndex > 0 {
		prev, ok := n.log.EntryAt(req.PrevLogIndex)
		if !ok || prev.Term != req.PrevLogTerm {
			resp := AppendEntriesResponse{Term: n.currentTerm, Success: false}
			n.mu.Unlock()
			return resp
		}
	}

	insertIndex := req.PrevLogIndex + 1
	for _, entry := range req.Entries {
		if existing, ok := n.log.EntryAt(insertIndex); ok {
			if existing.Term != entry.Term {
				if err := n.log.TruncateFrom(insertIndex); err != nil {
					resp := AppendEntriesResponse{Term: n.currentTerm, Success: false}
					n.mu.Unlock()
					return resp
				}
				n.log.Append(entry.Term, entry.Command)
			}
		} else {
			if n.log.LastIndex() != insertIndex-1 {
				resp := AppendEntriesResponse{Term: n.currentTerm, Success: false}
				n.mu.Unlock()
				return resp
			}
			n.log.Append(entry.Term, entry.Command)
		}
		insertIndex++
	}

	if req.LeaderCommit > n.commitIndex {
		lastIndex := n.log.LastIndex()
		if req.LeaderCommit < lastIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastIndex
		}
	}

	resp := AppendEntriesResponse{Term: n.currentTerm, Success: true}
	n.mu.Unlock()

	n.applyCommittedEntries()
	return resp
}

// advanceCommitIndex moves commit forward when a majority has replicated the entry.
func (n *Node) advanceCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	majority := len(n.cfg.Peers)/2 + 1

	for index := n.log.LastIndex(); index > n.commitIndex; index-- {
		entry, ok := n.log.EntryAt(index)
		if !ok || entry.Term != n.currentTerm {
			continue
		}

		replicas := 1
		for _, peer := range n.cfg.Peers {
			if peer == n.cfg.NodeID {
				continue
			}
			if n.matchIndex[peer] >= index {
				replicas++
			}
		}

		if replicas >= majority {
			n.commitIndex = index
			break
		}
	}
}

// applyCommittedEntries executes committed commands through the registered applier.
func (n *Node) applyCommittedEntries() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}

		n.lastApplied++
		index := n.lastApplied
		entry, ok := n.log.EntryAt(index)
		applier := n.applier
		n.mu.Unlock()

		if !ok || applier == nil || entry.Command.Op == "" {
			continue
		}
		applier(entry.Command)
	}
}
