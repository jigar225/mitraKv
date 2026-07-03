package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/jigar/mitrakv/internal/protocol"
	"github.com/jigar/mitrakv/internal/raft"
	"github.com/jigar/mitrakv/internal/store"
)

// handleConn serves one TCP client until EOF or error.
func (s *Server) handleConn(ctx context.Context, conn io.ReadWriter) {
	reader := bufio.NewReader(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cmd, err := protocol.Parse(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			slog.Warn("protocol parse failed", "err", err)
			return
		}

		start := time.Now()
		resp := s.dispatch(cmd)
		s.metrics.ObserveRequest(cmd.Name, time.Since(start))
		if writeErr := protocol.WriteString(conn, resp); writeErr != nil {
			slog.Warn("write response failed", "err", writeErr)
			return
		}
	}
}

func (s *Server) dispatch(cmd protocol.Command) string {
	switch cmd.Name {
	case "PING":
		return protocol.FormatPong()
	case "SET":
		if len(cmd.Args) != 2 {
			return protocol.FormatError("wrong number of arguments for SET")
		}
		if s.raft != nil {
			return s.proposeWrite(raft.Command{Op: raft.OpSet, Key: cmd.Args[0], Value: cmd.Args[1]})
		}
		if s.wal != nil {
			if err := s.wal.AppendSet(cmd.Args[0], cmd.Args[1]); err != nil {
				slog.Error("wal append set failed", "err", err, "key", cmd.Args[0])
				return protocol.FormatError("failed to persist write")
			}
		}
		s.store.Set(cmd.Args[0], cmd.Args[1])
		return protocol.FormatOK()
	case "GET":
		if len(cmd.Args) != 1 {
			return protocol.FormatError("wrong number of arguments for GET")
		}
		if val, ok := s.store.Get(cmd.Args[0]); ok {
			return protocol.FormatValue(val)
		}
		return protocol.FormatNil()
	case "DEL":
		if len(cmd.Args) != 1 {
			return protocol.FormatError("wrong number of arguments for DEL")
		}
		if s.raft != nil {
			return s.proposeWrite(raft.Command{Op: raft.OpDel, Key: cmd.Args[0]})
		}
		if s.wal != nil {
			if err := s.wal.AppendDel(cmd.Args[0]); err != nil {
				slog.Error("wal append del failed", "err", err, "key", cmd.Args[0])
				return protocol.FormatError("failed to persist write")
			}
		}
		s.store.Del(cmd.Args[0])
		return protocol.FormatOK()
	default:
		return protocol.FormatError("unknown command")
	}
}

// Store returns the underlying store for tests.
func (s *Server) Store() *store.Store {
	return s.store
}

func (s *Server) proposeWrite(cmd raft.Command) string {
	if err := s.raft.Propose(cmd); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return protocol.FormatError("not leader")
		}
		slog.Error("raft propose failed", "err", err, "op", cmd.Op, "key", cmd.Key)
		return protocol.FormatError("failed to replicate write")
	}
	return protocol.FormatOK()
}
