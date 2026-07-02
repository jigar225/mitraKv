package store

import "sync"

// Store is an in-memory key-value map safe for concurrent reads and writes.
// Phase 1 keeps everything in RAM; WAL persistence comes in Phase 2.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates an empty store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Set stores key with value, overwriting any existing value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value for key and whether it exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

// Del removes key if present and reports whether it existed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}
