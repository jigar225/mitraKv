package raft

import (
	"testing"
	"time"
)

func TestPeerCircuitBreakerOpensAfterThreshold(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	cb := newPeerCircuitBreaker(3, 30*time.Second)
	cb.now = clock

	for i := 0; i < 2; i++ {
		if !cb.allow() {
			t.Fatalf("attempt %d: expected breaker closed", i+1)
		}
		cb.recordFailure()
	}

	if !cb.allow() {
		t.Fatal("expected third attempt before open")
	}
	cb.recordFailure()

	if cb.allow() {
		t.Fatal("expected breaker open after threshold failures")
	}
}

func TestPeerCircuitBreakerHalfOpenProbeRecovers(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	cb := newPeerCircuitBreaker(1, 30*time.Second)
	cb.now = clock

	if !cb.allow() {
		t.Fatal("expected initial allow")
	}
	cb.recordFailure()
	if cb.allow() {
		t.Fatal("expected breaker open")
	}

	now = now.Add(31 * time.Second)
	if !cb.allow() {
		t.Fatal("expected half-open probe after cooldown")
	}
	if cb.allow() {
		t.Fatal("expected only one half-open probe in flight")
	}

	cb.recordSuccess()
	if !cb.allow() {
		t.Fatal("expected breaker closed after successful probe")
	}
}

func TestPeerCircuitBreakerHalfOpenFailureReopens(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	cb := newPeerCircuitBreaker(1, 10*time.Second)
	cb.now = clock

	cb.recordFailure()
	now = now.Add(11 * time.Second)

	if !cb.allow() {
		t.Fatal("expected half-open probe")
	}
	cb.recordFailure()

	if cb.allow() {
		t.Fatal("expected breaker open again after failed probe")
	}
}
