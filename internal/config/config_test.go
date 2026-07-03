package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadClusterConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node1.yaml")
	content := `node_id: node1
client_port: 6379
raft_port: 7379
metrics_port: 9090
data_dir: ./data/node1
peers:
  - node1: 127.0.0.1:7379
  - node2: 127.0.0.1:7380
  - node3: 127.0.0.1:7381
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NodeID != "node1" || cfg.ClientPort != 6379 || cfg.RaftPort != 7379 {
		t.Fatalf("cfg = %+v", cfg)
	}
	if len(cfg.PeerIDs) != 3 || cfg.PeerAddrs["node2"] != "127.0.0.1:7380" {
		t.Fatalf("peers = %+v", cfg.PeerAddrs)
	}
}
