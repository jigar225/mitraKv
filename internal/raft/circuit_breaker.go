package raft

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen means outbound RPCs to a peer are paused after repeated failures.
var ErrCircuitOpen = errors.New("circuit breaker open")

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

// peerCircuitBreaker pauses RPCs to a dead peer and probes recovery after a cooldown.
type peerCircuitBreaker struct {
	mu sync.Mutex

	state            breakerState
	consecutiveFails int
	failureThreshold int
	openDuration     time.Duration
	openedAt         time.Time
	halfOpenInFlight bool
	now              func() time.Time
}

func newPeerCircuitBreaker(failureThreshold int, openDuration time.Duration) *peerCircuitBreaker {
	return &peerCircuitBreaker{
		failureThreshold: failureThreshold,
		openDuration:     openDuration,
		now:              time.Now,
	}
}

// allow reports whether an RPC attempt should proceed for this peer.
func (b *peerCircuitBreaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if b.now().Sub(b.openedAt) >= b.openDuration {
			b.state = breakerHalfOpen
			b.halfOpenInFlight = false
			return b.tryHalfOpenLocked()
		}
		return false
	case breakerHalfOpen:
		return b.tryHalfOpenLocked()
	default:
		return false
	}
}

func (b *peerCircuitBreaker) tryHalfOpenLocked() bool {
	if b.halfOpenInFlight {
		return false
	}
	b.halfOpenInFlight = true
	return true
}

// recordSuccess closes the breaker after a successful RPC.
func (b *peerCircuitBreaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.state = breakerClosed
	b.consecutiveFails = 0
	b.halfOpenInFlight = false
}

// recordFailure counts toward opening the breaker; half-open probes reopen on failure.
func (b *peerCircuitBreaker) recordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == breakerHalfOpen {
		b.openLocked()
		return
	}

	b.consecutiveFails++
	if b.consecutiveFails >= b.failureThreshold {
		b.openLocked()
	}
}

func (b *peerCircuitBreaker) openLocked() {
	b.state = breakerOpen
	b.openedAt = b.now()
	b.consecutiveFails = 0
	b.halfOpenInFlight = false
}
