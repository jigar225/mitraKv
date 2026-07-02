package protocol

import (
	"bufio"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input string
		name  string
		args  []string
	}{
		{"SET name jigar\n", "SET", []string{"name", "jigar"}},
		{"GET name\n", "GET", []string{"name"}},
		{"DEL name\n", "DEL", []string{"name"}},
		{"PING\n", "PING", nil},
		{"ping\n", "PING", nil},
	}

	for _, tt := range tests {
		cmd, err := Parse(bufio.NewReader(strings.NewReader(tt.input)))
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		if cmd.Name != tt.name {
			t.Errorf("name = %q, want %q", cmd.Name, tt.name)
		}
		if len(cmd.Args) != len(tt.args) {
			t.Fatalf("args len = %d, want %d", len(cmd.Args), len(tt.args))
		}
		for i := range tt.args {
			if cmd.Args[i] != tt.args[i] {
				t.Errorf("args[%d] = %q, want %q", i, cmd.Args[i], tt.args[i])
			}
		}
	}
}
