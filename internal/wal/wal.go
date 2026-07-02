package wal

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// opSet exists so replay can distinguish overwrite semantics from deletes.
	opSet = "SET"
	// opDel exists so replay can remove keys exactly as the original write path did.
	opDel = "DEL"
)

// Log owns the append-only file so every write follows one durable path.
type Log struct {
	mu   sync.Mutex
	file *os.File
}

// Open ensures the WAL file is ready before serving writes, so durability starts at boot.
func Open(dataDir string) (*Log, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory %s: %w", dataDir, err)
	}

	path := filepath.Join(dataDir, "wal.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal file %s: %w", path, err)
	}

	return &Log{file: f}, nil
}

// Close releases the file descriptor so test runs and restarts do not leak OS resources.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}

	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close wal file: %w", err)
	}

	l.file = nil
	return nil
}

// AppendSet records SET before memory mutation so crash recovery can replay committed writes.
func (l *Log) AppendSet(key, value string) error {
	return l.append(opSet, key, value)
}

// AppendDel records DEL before memory mutation so delete intent survives process crashes.
func (l *Log) AppendDel(key string) error {
	return l.append(opDel, key, "")
}

func (l *Log) append(op, key, value string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return fmt.Errorf("wal is closed")
	}

	entry := fmt.Sprintf(
		"%s|%s|%s|%s\n",
		time.Now().UTC().Format(time.RFC3339Nano),
		op,
		base64.StdEncoding.EncodeToString([]byte(key)),
		base64.StdEncoding.EncodeToString([]byte(value)),
	)

	if _, err := l.file.WriteString(entry); err != nil {
		return fmt.Errorf("append wal entry: %w", err)
	}

	// Sync is the durability boundary; without it, a kernel crash can drop buffered writes.
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync wal entry: %w", err)
	}

	return nil
}

// Entry is shared with recovery so parsing and replay use one source of truth.
type Entry struct {
	Op        string
	Key       string
	Value     string
	Timestamp time.Time
}

// ParseLine keeps WAL decoding strict so corruption is surfaced early during replay.
func ParseLine(line string) (Entry, error) {
	parts := strings.Split(strings.TrimSpace(line), "|")
	if len(parts) != 4 {
		return Entry{}, fmt.Errorf("invalid wal line format")
	}

	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return Entry{}, fmt.Errorf("parse wal timestamp: %w", err)
	}

	keyBytes, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return Entry{}, fmt.Errorf("decode wal key: %w", err)
	}

	valueBytes, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return Entry{}, fmt.Errorf("decode wal value: %w", err)
	}

	return Entry{
		Op:        parts[1],
		Key:       string(keyBytes),
		Value:     string(valueBytes),
		Timestamp: ts,
	}, nil
}
