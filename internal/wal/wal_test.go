package wal

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/jigar/mitrakv/internal/store"
)

func TestAppendSetAndDel(t *testing.T) {
	dataDir := t.TempDir()

	log, err := Open(dataDir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() {
		if err := log.Close(); err != nil {
			t.Fatalf("close wal: %v", err)
		}
	}()

	if err := log.AppendSet("name", "jigar"); err != nil {
		t.Fatalf("append set: %v", err)
	}
	if err := log.AppendDel("name"); err != nil {
		t.Fatalf("append del: %v", err)
	}

	path := filepath.Join(dataDir, "wal.log")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open wal file for verify: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan wal lines: %v", err)
	}

	if len(lines) != 2 {
		t.Fatalf("wal line count = %d, want 2", len(lines))
	}

	e1, err := ParseLine(lines[0])
	if err != nil {
		t.Fatalf("parse line 1: %v", err)
	}
	if e1.Op != opSet || e1.Key != "name" || e1.Value != "jigar" {
		t.Fatalf("entry1 = %+v, want op=SET key=name value=jigar", e1)
	}

	e2, err := ParseLine(lines[1])
	if err != nil {
		t.Fatalf("parse line 2: %v", err)
	}
	if e2.Op != opDel || e2.Key != "name" || e2.Value != "" {
		t.Fatalf("entry2 = %+v, want op=DEL key=name value=''", e2)
	}
}

func TestParseLineRejectsInvalidFormat(t *testing.T) {
	if _, err := ParseLine("invalid-line"); err == nil {
		t.Fatal("ParseLine should fail for malformed input")
	}
}

func TestParseLineRejectsUnknownOperation(t *testing.T) {
	line := "2026-07-02T12:00:00Z|PATCH|Y2V5|dmFsdWU="
	if _, err := ParseLine(line); err == nil {
		t.Fatal("ParseLine should fail for unknown operation")
	}
}

func TestReplaySkipsBlankLines(t *testing.T) {
	dataDir := t.TempDir()

	log, err := Open(dataDir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := log.AppendSet("k", "v"); err != nil {
		t.Fatalf("append set: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	path := filepath.Join(dataDir, "wal.log")
	existing, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	// Inject blank lines between entries to mimic trailing newlines or empty appends.
	padded := append(existing, '\n', '\n')
	if err := os.WriteFile(path, padded, 0o644); err != nil {
		t.Fatalf("write padded wal: %v", err)
	}

	st := store.New()
	if err := Replay(dataDir, st); err != nil {
		t.Fatalf("replay with blank lines: %v", err)
	}
	if got, ok := st.Get("k"); !ok || got != "v" {
		t.Fatalf("Get k = %q, %v; want v, true", got, ok)
	}
}

func TestReplayFailsOnCorruptLine(t *testing.T) {
	dataDir := t.TempDir()
	path := filepath.Join(dataDir, "wal.log")
	if err := os.WriteFile(path, []byte("this-is-not-a-valid-wal-line\n"), 0o644); err != nil {
		t.Fatalf("write corrupt wal: %v", err)
	}

	st := store.New()
	if err := Replay(dataDir, st); err == nil {
		t.Fatal("Replay should fail on corrupt WAL line")
	}
}
