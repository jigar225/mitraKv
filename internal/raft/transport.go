package raft

// Transport sends Raft RPCs between cluster peers. Tests use an in-memory implementation.
type Transport interface {
	RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
