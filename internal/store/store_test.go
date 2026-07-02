package store

import (
	"sync"
	"testing"
)

func TestSetGetDel(t *testing.T) {
	s := New()

	s.Set("name", "jigar")
	val, ok := s.Get("name")
	if !ok || val != "jigar" {
		t.Fatalf("Get name = %q, %v; want jigar, true", val, ok)
	}

	if !s.Del("name") {
		t.Fatal("Del should return true for existing key")
	}
	if _, ok := s.Get("name"); ok {
		t.Fatal("key should be gone after Del")
	}
	if s.Del("name") {
		t.Fatal("Del on missing key should return false")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "k"
			s.Set(key, "v")
			s.Get(key)
			s.Del(key)
		}(i)
	}
	wg.Wait()
}
