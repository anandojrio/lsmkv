# lsmkv — Project completion status

**Last updated:** 2026-08-11  
**Module:** `github.com/anandojrio/lsmkv`  
**Stack:** Go · WAL · Memtable · SSTables · Manifest/Version · size-tiered compaction  

This file tracks what is implemented versus what remains for full course-spec completion.  
Ground truth is always the code and `go test ./...`, not older progress notes.

---

## Summary

| Area | Status |
|------|--------|
| Section 0 — Skeleton / CLI / config | **Done** |
| Section 1 — WAL & recovery | **Done** |
| Section 2 — Memtable + rotation + backpressure | **Done** |
| Section 3 — SSTable writer | **Done** |
| Section 4 — SSTable reader + global Get | **Done** |
| Section 5 — Manifest & Version | **Done** |
| Section 6 — Compaction (basic size-tiered) | **Done (basic)** |
| Section 7 — Concurrency & scheduling | **Done (+ polish)** |
| Section 8 — Observability | **Not started** |
| Section 9 — Fault injection / chaos | **Not started** |

**Engine is usable end-to-end:** put/get/del survive restart, flush to SST, compact, background workers, CLI.

---

## Done (by unit)

### Unit 0 — Skeleton, config, CLI
- [x] Repo layout (`cmd/lsmkv`, `internal/lsm`, `config/`, `data/`)
- [x] `Config` load/validate + `config/default.json`
- [x] CLI: `init`, `put`, `get`, `del`, `stats`, `flush`, `compact`, `close`, `bg-status`, `run` (legacy), `help`
- [x] Flags: `--config`, `--key`, `--value`, `--fast` (close)
- [x] Errors package / API surface on `Store`

### Unit 1 — WAL & crash recovery
- [x] Segmented WAL (`000001.wal`, …), roll by size
- [x] Record encode/decode + checksums
- [x] Append path with `walfsynceveryn`
- [x] Replay with truncated-tail handling
- [x] Reset/remove old segments after safe flush
- [x] Recovery drives memtable rebuild on `Open`

### Unit 2 — Memtable
- [x] Active memtable (put/del/get, tombstones, seqNo)
- [x] Size accounting (`Bytes`, `Len`)
- [x] Rotation when `memtable_max_bytes` exceeded
- [x] Immutable list + `max_immutable_tables` → `ErrTooManyImmutables`
- [x] Get order: active → immutables (newest first)

### Unit 3 — SSTable writer
- [x] Sorted entries → data blocks + index + Bloom + footer
- [x] Per-entry CRC, temp file + fsync + atomic rename
- [x] Wired into flush path

### Unit 4 — SSTable reader + reads
- [x] Bloom → index → one data block scan
- [x] Corruption → `ErrCorruptionDetected`
- [x] `Store.Get` falls through to `Version` (newest SST first)
- [x] Tombstones hide older values

### Unit 5 — Manifest & Version
- [x] Durable `manifest.json` (load/save, atomic replace)
- [x] Immutable `Version` snapshot of live SST readers
- [x] Publish on flush and compaction
- [x] Startup: load manifest → open SSTs → WAL replay

### Unit 6 — Compaction (basic)
- [x] Merge two oldest tables (size-tiered starter)
- [x] Newest-wins merge; tombstones preserved in merge output
- [x] Manifest rewrite + Version rebuild
- [x] `Store.Compact()` + CLI `compact`
- [ ] Advanced picker (fan-in > 2, size-ratio bands) — **optional next**
- [ ] Explicit tombstone drop when no older version remains — **partial/basic only**
- [ ] Leveled layout / true L0 vs Ln — **not required if size-tiered is accepted**

### Unit 7 — Concurrency & scheduling (+ polish)
- [x] Single-writer mutex on mutations
- [x] Background flush worker (queue + non-blocking enqueue)
- [x] Background compact worker
- [x] `ForceFlush` / `FlushAll` synchronous drain paths
- [x] Auto-compact when `live SST count >= l0_compaction_trigger` (after **bg** flush only)
- [x] `l0_compaction_trigger` config (`0` = disable)
- [x] `BGStatus` + CLI `bg-status`
- [x] Graceful close (stop workers → drain immutables → close)
- [x] Fast close (`CloseFast` / `close --fast`, WAL recovery)
- [x] `lastBGError` surfaced via `Stats.EngineStatus` when degraded
- [x] Tests updated for async flush and deterministic compaction
- [ ] IO throttle (`compaction_io_mb_per_s`) — **not done**
- [ ] Write stall on too many L0 files (`l0_stop_writes`) — **not done**
- [ ] Multi-concurrent compaction workers — **not done** (one compact worker is enough for course)

---

## Not done (remaining for “full spec” completion)

### Unit 8 — Observability
- [ ] Rich metrics set (e.g. writes total, bloom checks/skips, block reads, flush/compact durations histograms)
- [ ] Structured log events (`rotate_memtable`, `flush_start`, `flush_publish`, `compact_job`, `stall`)
- [ ] CLI inspectors: `manifest-info`, `version-info`, `list-sst` (and wire `stats` to more counters)
- [ ] Optional: recovery report printed on every `Open` in a stable one-line format

### Unit 9 — Testing & fault injection
- [ ] Crash points (e.g. after SST data blocks before index; after SST rename before manifest save)
- [ ] Scripted kill/reopen drills with expected invariants
- [ ] Documented recipe per drill (README or `docs/chaos.md`)
- [ ] CI job running `go test ./...` (if not already)

### Hardening / nice-to-haves (spec-adjacent)
- [ ] Block cache for SST data/index blocks
- [ ] Compaction tombstone GC policy with grace period
- [ ] Size-tiered picker generalization (N-way merge, size ratio)
- [ ] `wal-verify` CLI utility
- [ ] Refresh `lsmkv-progress.md` so it matches the repo (it was historically stale)

---

## Suggested finish order

1. **Unit 8 (observability)** — small surface area; makes demos and grading easier.  
2. **Unit 9 (fault injection)** — proves durability claims under crash.  
3. **Compaction depth** — only if the course rubric demands fan-in / tombstone drop / levels beyond the basic two-table merge.

---

## How to verify current tree

```powershell
go build ./...
go test ./... -v
go build -o lsmkv.exe ./cmd/lsmkv
.\lsmkv.exe help
.\lsmkv.exe init
.\lsmkv.exe put --key hello --value world
.\lsmkv.exe get --key hello
.\lsmkv.exe stats
.\lsmkv.exe bg-status
.\lsmkv.exe flush
.\lsmkv.exe compact
.\lsmkv.exe close
```

---

## Checklist legend

- **Done** — implemented and covered by passing tests or manual CLI path.  
- **Basic** — meets core course behavior; advanced knobs may still be open.  
- **Not started** — no meaningful implementation yet.