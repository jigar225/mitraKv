package benchmarks

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jigar/mitrakv/internal/metrics"
	"github.com/jigar/mitrakv/internal/server"
	"github.com/jigar/mitrakv/internal/store"
)

const concurrentWorkers = 1000

// BenchmarkGET measures read latency under concurrent load on a single node.
func BenchmarkGET(b *testing.B) {
	addr := startBenchServer(b)
	runConcurrentBench(b, addr, concurrentWorkers, func(int) (time.Duration, error) {
		return roundTrip(addr, "GET benchkey\n")
	})
}

// BenchmarkSET measures write latency under concurrent load on a single node.
func BenchmarkSET(b *testing.B) {
	addr := startBenchServer(b)
	runConcurrentBench(b, addr, concurrentWorkers, func(int) (time.Duration, error) {
		return roundTrip(addr, "SET benchkey value\n")
	})
}

// BenchmarkMixed measures an 80/20 read/write workload like real KV traffic.
func BenchmarkMixed(b *testing.B) {
	addr := startBenchServer(b)
	runConcurrentBench(b, addr, concurrentWorkers, func(idx int) (time.Duration, error) {
		if idx%5 == 0 {
			return roundTrip(addr, "SET benchkey value\n")
		}
		return roundTrip(addr, "GET benchkey\n")
	})
}

func startBenchServer(b *testing.B) string {
	b.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		b.Fatalf("close listener: %v", err)
	}

	recorder, err := metrics.NewRecorder()
	if err != nil {
		b.Fatalf("metrics: %v", err)
	}

	srv := server.New(addr, store.New(), recorder, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	waitForTCP(b, addr)
	return addr
}

func runConcurrentBench(b *testing.B, addr string, workers int, op func(int) (time.Duration, error)) {
	b.Helper()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		latencies := make([]time.Duration, workers)
		var wg sync.WaitGroup
		wg.Add(workers)

		for w := 0; w < workers; w++ {
			go func(idx int) {
				defer wg.Done()
				d, err := op(idx)
				if err != nil {
					b.Errorf("request failed: %v", err)
					return
				}
				latencies[idx] = d
			}(w)
		}
		wg.Wait()

		reportPercentiles(b, latencies)
	}
}

func reportPercentiles(b *testing.B, latencies []time.Duration) {
	b.Helper()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Microseconds())/1000, "p50_ms")
	b.ReportMetric(float64(p95.Microseconds())/1000, "p95_ms")
	b.ReportMetric(float64(p99.Microseconds())/1000, "p99_ms")
}

func roundTrip(addr, command string) (time.Duration, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	start := time.Now()
	if _, err := conn.Write([]byte(command)); err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}

	reader := bufio.NewReader(conn)
	if _, err := reader.ReadString('\n'); err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	return time.Since(start), nil
}

func waitForTCP(b *testing.B, addr string) {
	b.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	b.Fatalf("server not listening on %s", addr)
}
