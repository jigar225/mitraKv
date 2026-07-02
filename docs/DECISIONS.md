# MitraKV — Decision Log

Record *what* was decided, *why*, and *when*. Keep entries short.

---

## Format

```
### [YYYY-MM-DD] Title
**Decision:** ...
**Why:** ...
**Alternatives considered:** ...
```

---

## Decisions

### [2026-06-27] Repo-native session handoff
**Decision:** Use `PROGRESS.md` (status + next steps) and this file (decisions) instead of relying on chat history.
**Why:** Context windows fill up; the repo persists across sessions, models, and machines. Industry pattern (Anthropic harness engineering, checkpoint-not-transcript).
**Alternatives considered:** Pasting full chat summaries each session (wastes tokens, loses structure).

### [2026-06-27] Build order follows PRD phases
**Decision:** Phase 1 → 2 → 3 → 4 strictly. No Raft until single-node + WAL work.
**Why:** Each layer depends on the one below. WAL needs a working store; Raft needs WAL.
**Alternatives considered:** Jumping to Raft early (tempting but hides bugs in lower layers).

### [2026-06-27] Pure stdlib + Prometheus only
**Decision:** No Redis/etcd/Raft libraries. Only `prometheus/client_golang` for metrics.
**Why:** PRD goal is understanding, not wiring. Interview story is "built from scratch."
**Alternatives considered:** `hashicorp/raft` (faster but defeats the learning goal).
