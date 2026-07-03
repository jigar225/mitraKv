package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// SetTransport wires peer communication so elections can request votes remotely.
func (n *Node) SetTransport(t Transport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.transport = t
}

// HandleRequestVote applies Raft vote rules so only one leader emerges per term.
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	if n.votedFor != "" && n.votedFor != req.CandidateID {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	if !logIsAtLeastAsUpToDate(req.LastLogTerm, req.LastLogIndex, n.log.LastTerm(), n.log.LastIndex()) {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	n.votedFor = req.CandidateID
	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}
}

// NotifyHeartbeat resets the follower election timer when a valid leader is heard from.
func (n *Node) NotifyHeartbeat(term uint64, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term >= n.currentTerm {
		n.becomeFollower(term)
		n.leaderID = leaderID
	}

	select {
	case n.electionReset <- struct{}{}:
	default:
	}
}

// Run drives election timeouts and leader heartbeats until ctx is cancelled.
func (n *Node) Run(ctx context.Context) error {
	electionTimer := time.NewTimer(n.randomElectionTimeout())
	defer electionTimer.Stop()

	heartbeat := time.NewTicker(n.cfg.HeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeat.C:
			if n.IsLeader() {
				n.replicateToPeers()
			}
		case <-n.electionReset:
			if !n.IsLeader() {
				if !electionTimer.Stop() {
					select {
					case <-electionTimer.C:
					default:
					}
				}
				electionTimer.Reset(n.randomElectionTimeout())
			}
		case <-electionTimer.C:
			if n.IsLeader() {
				electionTimer.Reset(n.randomElectionTimeout())
				continue
			}
			if n.startElection() {
				slog.Info("raft leader elected", "node_id", n.cfg.NodeID, "term", n.CurrentTerm())
				n.replicateToPeers()
			}
			electionTimer.Reset(n.randomElectionTimeout())
		}
	}
}

func (n *Node) startElection() bool {
	n.mu.Lock()
	n.becomeCandidate()
	term := n.currentTerm
	lastIndex := n.log.LastIndex()
	lastTerm := n.log.LastTerm()
	candidateID := n.cfg.NodeID
	transport := n.transport
	peers := append([]string(nil), n.cfg.Peers...)
	n.mu.Unlock()

	if transport == nil {
		return false
	}

	votes := 1
	var votesMu sync.Mutex
	majority := len(peers)/2 + 1

	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer == candidateID {
			continue
		}
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()

			resp, err := transport.RequestVote(peerID, RequestVoteRequest{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				slog.Warn("request vote failed", "peer", peerID, "err", err)
				return
			}

			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if resp.VoteGranted {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()

	if n.currentTerm != term || n.state != Candidate {
		n.mu.Unlock()
		return false
	}
	if votes >= majority {
		n.becomeLeader()
		n.leaderID = n.cfg.NodeID
		n.mu.Unlock()
		return true
	}
	n.mu.Unlock()
	return false
}

func (n *Node) randomElectionTimeout() time.Duration {
	min := n.cfg.ElectionTimeoutMin
	max := n.cfg.ElectionTimeoutMax
	if min == 0 {
		min = 150 * time.Millisecond
	}
	if max == 0 {
		max = 300 * time.Millisecond
	}
	if max <= min {
		return min
	}
	delta := max - min
	return min + time.Duration(rand.Int63n(int64(delta)))
}

// logIsAtLeastAsUpToDate compares candidate and voter logs per Raft's election safety rule.
func logIsAtLeastAsUpToDate(candidateLastTerm, candidateLastIndex, voterLastTerm, voterLastIndex uint64) bool {
	if candidateLastTerm != voterLastTerm {
		return candidateLastTerm > voterLastTerm
	}
	return candidateLastIndex >= voterLastIndex
}
