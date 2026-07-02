package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Command represents a parsed client request on the wire.
type Command struct {
	Name string
	Args []string
}

// Parse reads one line from r and returns the command.
// Wire format: "SET key value", "GET key", "DEL key", "PING".
func Parse(r *bufio.Reader) (Command, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return Command{}, err
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return Command{}, fmt.Errorf("empty command")
	}

	parts := strings.Fields(line)
	if len(parts) == 0 {
		return Command{}, fmt.Errorf("empty command")
	}

	return Command{Name: strings.ToUpper(parts[0]), Args: parts[1:]}, nil
}

// FormatOK returns the success response for mutating commands.
func FormatOK() string {
	return "OK\n"
}

// FormatValue returns a GET hit response.
func FormatValue(value string) string {
	return value + "\n"
}

// FormatNil returns a GET miss response.
func FormatNil() string {
	return "(nil)\n"
}

// FormatPong returns the PING response.
func FormatPong() string {
	return "PONG\n"
}

// FormatError returns a client-visible error line.
func FormatError(msg string) string {
	return "ERR " + msg + "\n"
}

// WriteString writes s to w; used by handlers to send responses.
func WriteString(w io.Writer, s string) error {
	_, err := io.WriteString(w, s)
	return err
}
