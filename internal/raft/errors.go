package raft

import "errors"

// ErrNotLeader is returned when a client write hits a follower instead of the leader.
var ErrNotLeader = errors.New("raft: not leader")

// ErrCommitTimeout is returned when a proposed entry is not committed in time.
var ErrCommitTimeout = errors.New("raft: commit timeout")
