package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	rpcRequestVote   = "RequestVote"
	rpcAppendEntries = "AppendEntries"
)

type rpcEnvelope struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload"`
}

// writeRPC sends one request/response as two newline-terminated lines.
func writeRPC(w io.Writer, method string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal rpc payload: %w", err)
	}
	if _, err := fmt.Fprintf(w, "%s\n%s\n", method, body); err != nil {
		return fmt.Errorf("write rpc: %w", err)
	}
	return nil
}

func readRPC(r *bufio.Reader, dest any) (string, error) {
	method, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read rpc method: %w", err)
	}
	method = strings.TrimSpace(method)

	body, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read rpc body: %w", err)
	}
	body = strings.TrimSpace(body)

	if err := json.Unmarshal([]byte(body), dest); err != nil {
		return "", fmt.Errorf("decode rpc body: %w", err)
	}
	return method, nil
}
