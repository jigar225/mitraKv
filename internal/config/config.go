package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Node holds runtime settings for one MitraKV cluster member.
type Node struct {
	NodeID      string
	ClientPort  int
	RaftPort    int
	MetricsPort int
	DataDir     string
	PeerIDs     []string
	PeerAddrs   map[string]string
}

// Load reads a simple YAML-like config file (stdlib only, no YAML parser).
func Load(path string) (Node, error) {
	file, err := os.Open(path)
	if err != nil {
		return Node{}, fmt.Errorf("open config %s: %w", path, err)
	}
	defer file.Close()

	cfg := Node{PeerAddrs: make(map[string]string)}
	inPeers := false

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if line == "peers:" {
			inPeers = true
			continue
		}

		if inPeers {
			if strings.HasPrefix(line, "-") {
				peerLine := strings.TrimSpace(strings.TrimPrefix(line, "-"))
				parts := strings.SplitN(peerLine, ":", 2)
				if len(parts) != 2 {
					return Node{}, fmt.Errorf("invalid peer line %q", line)
				}
				id := strings.TrimSpace(parts[0])
				addr := strings.TrimSpace(parts[1])
				cfg.PeerIDs = append(cfg.PeerIDs, id)
				cfg.PeerAddrs[id] = addr
				continue
			}
			inPeers = false
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return Node{}, fmt.Errorf("invalid config line %q", line)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "node_id":
			cfg.NodeID = value
		case "data_dir":
			cfg.DataDir = value
		case "client_port":
			cfg.ClientPort, err = strconv.Atoi(value)
		case "raft_port":
			cfg.RaftPort, err = strconv.Atoi(value)
		case "metrics_port":
			cfg.MetricsPort, err = strconv.Atoi(value)
		default:
			return Node{}, fmt.Errorf("unknown config key %q", key)
		}
		if err != nil {
			return Node{}, fmt.Errorf("parse %s: %w", key, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return Node{}, fmt.Errorf("read config: %w", err)
	}
	if cfg.NodeID == "" {
		return Node{}, fmt.Errorf("node_id is required")
	}
	if len(cfg.PeerIDs) == 0 {
		return Node{}, fmt.Errorf("at least one peer is required")
	}

	return cfg, nil
}
