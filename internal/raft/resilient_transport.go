package raft

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ResilientTransport wraps a Transport with per-peer circuit breakers and retry backoff.
type ResilientTransport struct {
	inner Transport

	mu       sync.Mutex
	breakers map[string]*peerCircuitBreaker

	maxRetries     int
	initialBackoff time.Duration
}

// NewResilientTransport adds failure-aware retries around outbound Raft RPCs.
func NewResilientTransport(inner Transport) *ResilientTransport {
	return &ResilientTransport{
		inner:          inner,
		breakers:       make(map[string]*peerCircuitBreaker),
		maxRetries:     3,
		initialBackoff: 100 * time.Millisecond,
	}
}

func (t *ResilientTransport) RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	var resp RequestVoteResponse
	err := t.withRetry(peerID, func() error {
		var callErr error
		resp, callErr = t.inner.RequestVote(peerID, req)
		return callErr
	})
	return resp, err
}

func (t *ResilientTransport) AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	err := t.withRetry(peerID, func() error {
		var callErr error
		resp, callErr = t.inner.AppendEntries(peerID, req)
		return callErr
	})
	return resp, err
}

func (t *ResilientTransport) breakerFor(peerID string) *peerCircuitBreaker {
	t.mu.Lock()
	defer t.mu.Unlock()

	b, ok := t.breakers[peerID]
	if !ok {
		b = newPeerCircuitBreaker(3, 30*time.Second)
		t.breakers[peerID] = b
	}
	return b
}

// withRetry attempts an RPC up to maxRetries with exponential backoff before opening the breaker.
func (t *ResilientTransport) withRetry(peerID string, fn func() error) error {
	cb := t.breakerFor(peerID)
	backoff := t.initialBackoff

	var lastErr error
	for attempt := 0; attempt < t.maxRetries; attempt++ {
		if !cb.allow() {
			return fmt.Errorf("%w: peer %s", ErrCircuitOpen, peerID)
		}

		lastErr = fn()
		if lastErr == nil {
			cb.recordSuccess()
			return nil
		}

		cb.recordFailure()
		if attempt < t.maxRetries-1 && cb.allow() {
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	slog.Warn("peer rpc failed after retries",
		"peer", peerID,
		"attempts", t.maxRetries,
		"err", lastErr,
	)
	return lastErr
}
