package raft

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type flakyTransport struct {
	failures atomic.Int32
}

func (f *flakyTransport) RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	if f.failures.Add(1) <= 2 {
		return RequestVoteResponse{}, errors.New("peer down")
	}
	return RequestVoteResponse{VoteGranted: true}, nil
}

func (f *flakyTransport) AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Success: true}, nil
}

func TestResilientTransportRetriesWithBackoff(t *testing.T) {
	inner := &flakyTransport{}
	transport := NewResilientTransport(inner)
	transport.initialBackoff = 5 * time.Millisecond

	start := time.Now()
	resp, err := transport.RequestVote("node2", RequestVoteRequest{Term: 1, CandidateID: "node1"})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatal("expected vote granted after retries")
	}
	if elapsed < 10*time.Millisecond {
		t.Fatalf("expected backoff delay, got %v", elapsed)
	}
}

type alwaysFailTransport struct {
	calls atomic.Int32
}

func (f *alwaysFailTransport) RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	f.calls.Add(1)
	return RequestVoteResponse{}, errors.New("peer down")
}

func (f *alwaysFailTransport) AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	f.calls.Add(1)
	return AppendEntriesResponse{}, errors.New("peer down")
}

func TestResilientTransportOpensCircuitAfterRetries(t *testing.T) {
	inner := &alwaysFailTransport{}
	transport := NewResilientTransport(inner)
	transport.initialBackoff = time.Millisecond

	_, err := transport.AppendEntries("node2", AppendEntriesRequest{Term: 1, LeaderID: "node1"})
	if err == nil {
		t.Fatal("expected error after retries")
	}

	firstBurst := inner.calls.Load()
	if firstBurst != 3 {
		t.Fatalf("expected 3 attempts in first burst, got %d", firstBurst)
	}

	_, err = transport.AppendEntries("node2", AppendEntriesRequest{Term: 1, LeaderID: "node1"})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected circuit open, got %v", err)
	}
	if inner.calls.Load() != firstBurst {
		t.Fatal("expected no extra dials while circuit is open")
	}
}
