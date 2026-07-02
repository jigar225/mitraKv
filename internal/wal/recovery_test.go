package wal

import (
	"testing"

	"github.com/jigar/mitrakv/internal/store"
)

func TestReplayRestoresLatestState(t *testing.T) {
	dataDir := t.TempDir()

	log, err := Open(dataDir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	if err := log.AppendSet("name", "jigar"); err != nil {
		t.Fatalf("append set 1: %v", err)
	}
	if err := log.AppendSet("name", "rahul"); err != nil {
		t.Fatalf("append set 2: %v", err)
	}
	if err := log.AppendSet("city", "mumbai"); err != nil {
		t.Fatalf("append set city: %v", err)
	}
	if err := log.AppendDel("city"); err != nil {
		t.Fatalf("append del city: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	restored := store.New()
	if err := Replay(dataDir, restored); err != nil {
		t.Fatalf("replay wal: %v", err)
	}

	if got, ok := restored.Get("name"); !ok || got != "rahul" {
		t.Fatalf("name after replay = %q, %v; want rahul, true", got, ok)
	}
	if _, ok := restored.Get("city"); ok {
		t.Fatal("city should be deleted after replay")
	}
}

func TestReplayMissingWalFileIsNoop(t *testing.T) {
	st := store.New()
	if err := Replay(t.TempDir(), st); err != nil {
		t.Fatalf("Replay should not fail when wal file is absent: %v", err)
	}
}
