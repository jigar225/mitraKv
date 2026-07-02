package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jigar/mitrakv/internal/store"
)

// Replay restores in-memory state from WAL so restarts keep previously committed writes.
func Replay(dataDir string, st *store.Store) error {
	path := filepath.Join(dataDir, "wal.log")
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open wal for replay %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		entry, parseErr := ParseLine(scanner.Text())
		if parseErr != nil {
			return fmt.Errorf("parse wal line %d: %w", lineNum, parseErr)
		}

		switch entry.Op {
		case opSet:
			st.Set(entry.Key, entry.Value)
		case opDel:
			st.Del(entry.Key)
		default:
			return fmt.Errorf("unknown wal operation %q on line %d", entry.Op, lineNum)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan wal file %s: %w", path, err)
	}

	return nil
}
