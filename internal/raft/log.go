package raft

import "fmt"

// Log is the replicated append-only sequence every node must agree on.
type Log struct {
	entries []LogEntry
}

// NewLog creates an empty Raft log with a sentinel entry at index 0 (term 0).
// The sentinel keeps 1-based indexing aligned with the Raft paper.
func NewLog() *Log {
	return &Log{
		entries: []LogEntry{{Index: 0, Term: 0}},
	}
}

// LastIndex returns the index of the newest entry (0 when only sentinel exists).
func (l *Log) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the newest entry (0 when only sentinel exists).
func (l *Log) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// EntryAt returns the log entry at index. Index 0 is the sentinel.
func (l *Log) EntryAt(index uint64) (LogEntry, bool) {
	if index >= uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

// Append adds entries for the given term, assigning consecutive indices.
func (l *Log) Append(term uint64, commands ...Command) []LogEntry {
	nextIndex := l.LastIndex() + 1
	appended := make([]LogEntry, 0, len(commands))

	for i, cmd := range commands {
		entry := LogEntry{
			Index:   nextIndex + uint64(i),
			Term:    term,
			Command: cmd,
		}
		l.entries = append(l.entries, entry)
		appended = append(appended, entry)
	}

	return appended
}

// TruncateFrom drops entries at index and beyond so a follower can realign with the leader.
func (l *Log) TruncateFrom(index uint64) error {
	if index == 0 {
		return fmt.Errorf("cannot truncate sentinel entry")
	}
	if index > l.LastIndex() {
		return fmt.Errorf("truncate index %d beyond last index %d", index, l.LastIndex())
	}

	l.entries = l.entries[:index]
	return nil
}

// EntriesFrom returns log entries starting at index through the end (inclusive).
func (l *Log) EntriesFrom(index uint64) []LogEntry {
	if index > l.LastIndex() {
		return nil
	}
	out := make([]LogEntry, 0, l.LastIndex()-index+1)
	for i := index; i <= l.LastIndex(); i++ {
		out = append(out, l.entries[i])
	}
	return out
}

// Len returns total entries including the sentinel at index 0.
func (l *Log) Len() int {
	return len(l.entries)
}
