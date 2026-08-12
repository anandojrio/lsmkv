# Handoff guide — continue Phase A (8–9) and Phase B

**From:** Matija (LSM engine Units 0–7)  
**To:** Partner  
**Module:** Go, `internal/lsm` + `cmd/lsmkv`

Read this + `docs/PROJECT_STATUS.md`, then run the verify commands. Do not trust outdated progress markdown at repo root without diffing against code.

---

## 1. What you are inheriting

A **working single-node LSM KV store**:

- Durable Put/Delete via WAL  
- Memtable + immutable rotation + backpressure  
- Flush → SSTable + manifest + Version  
- Get across memory and SSTs  
- **Size-tiered compaction** (configurable fan-in / size ratio)  
- **Background flush & compact workers**  
- Write stall when too many SSTs (`l0_stop_writes`)  
- CLI for day-to-day ops  
- Unit tests covering core paths (must stay green)

You own finishing **Phase A §8–9** and all of **Phase B**.

---

## 2. Layout (where to look)

```text
cmd/lsmkv/main.go          CLI
config/default.json        defaults
internal/lsm/
  store.go                 Open, Put, Get, Delete, Compact, ForceFlush, Close*
  scheduler.go             flush/compact workers, queues, BGStatus
  compaction.go            picker, runCompactionOnce, compactReaders, manifest edit
  compaction_merge.go      mergeEntries (newest seq wins)
  wal*.go, memtable.go     write path durability + memory
  sstable*.go, bloom.go    on-disk tables
  manifest.go, version.go  atomic file set + read snapshot
  config.go, errors.go, stats.go
```

**Data flow**

```text
Put/Delete
  → (optional write stall)
  → WAL.Append
  → memtable
  → rotate → immutables → flushQueue
       → flush worker → SST + manifest + Version
       → maybeEnqueueCompact
            → compact worker → picker → merge → new SST + manifest + Version

Get → mem → immutables (newest first) → version.SSTables (newest first)
```

---

## 3. Config knobs that matter

| JSON key | Role |
|----------|------|
| `memtable_max_bytes` | Rotation threshold |
| `max_immutable_tables` | Write backpressure (`ErrTooManyImmutables`) |
| `size_tiered_fan_in` | K tables per compaction job (default 4) |
| `size_tiered_size_ratio` | Max size spread in a pick (default 2.0) |
| `tombstone_grace_seconds` | **Reserved** — compaction does **not** drop tombstones yet |
| `l0_compaction_trigger` | Auto-compact when live SST count ≥ N; **0 = off** |
| `l0_stop_writes` | Put/Delete → `ErrWriteStall` when SST count ≥ N; **0 = off** |
| WAL fsync / segment roll | Durability vs throughput |

Document any default you change (trigger may be 4 or 8 depending on tree).

---

## 4. Important APIs

| API | Behavior |
|-----|----------|
| `ForceFlush` | Rotate non-empty active if needed; **sync** drain immutables; **does not** auto-compact |
| `Compact` | One picker job; **no-op** if immutables > 0 |
| `Close` / `CloseGraceful` | Stop workers; drain immutables; close WAL/Version |
| `CloseFast` | Stop workers quickly; unflushed data remains in WAL for recovery |
| `BGStatus` | Queues, job counts, last durations, last error, trigger |

Errors: `ErrStoreClosed`, `ErrInvalidArgument`, `ErrNotFound`, `ErrCorruptionDetected`, `ErrTooManyImmutables`, `ErrWriteStall`.

---

## 5. Compaction & scheduling contracts

**Picker (`pickSizeTiered`)**  
1. ≥ 2 tables  
2. Sort by `FileSize` asc, tie-break higher ID  
3. Window of K within `size_ratio` of smallest in window  
4. Else up to K newest by ID  
5. Return newest-first  

**Merge**  
Highest `seqNo` wins; **keep tombstones** (required for future replication).

**Crash safety**  
Write output SST (temp+rename) → save manifest → delete inputs.  
Crash before manifest: output may be orphan; inputs still authoritative.

**Workers**  
- 1 flush worker, 1 compact worker, bounded channels  
- Flush completion may enqueue compact if `liveSSTCount >= trigger`  
- Compact worker refuses to run while `immutableCount() > 0`

---

## 6. Your remaining Phase A work

### Unit 8 — Observability
- Counters/histograms (writes, bloom checks/skips, block reads, flush/compact timing)
- Structured log lines (`rotate`, `flush publish`, `compact job`, `stall`)
- CLI: `manifest-info`, `list-sst`, richer `stats` (optional `version-info`)
- Do not change publish/crash order when adding metrics

### Unit 9 — Fault injection
- Named crash points (after SST rename before manifest; mid-compact before manifest; WAL tail; etc.)
- Restart + invariant checks (no lost acked writes per fsync policy; no torn Version)
- Short `docs/chaos.md` recipes
- Keep tests deterministic (`-count=1`, temp dirs via `testConfig`)

---

## 7. Phase B (later) — what this engine already gives you

Spec themes: consistent hashing, replication, R/W quorums, hinted handoff, anti-entropy, conflict resolution.

| Engine guarantee | Why Phase B cares |
|------------------|-------------------|
| Tombstones retained | Deletes must replicate / repair |
| `seqNo` monotonic per node | Ordering / last-write-wins baseline |
| Newest-first reads | Single-node “first hit wins” |
| Crash-safe SST + manifest | Replica SST exchange won’t half-publish locally |
| Stable `Store` API | Wrap with RPC without rewriting LSM |

Phase B should **wrap** `Store` (or a thin server interface), not fork compaction/WAL semantics. If you add vector clocks, keep local durability path intact.

---

## 8. Do not break (regression bar)

Before every PR:

```text
go build ./...
go test ./internal/lsm -count=1
go test ./... -count=1
```

Manually: put/get across restart; flush; compact; `bg-status`; stall (set `l0_stop_writes` low); graceful vs fast close.

---

## 9. Known non-goals / debt

- No block cache  
- No leveled compaction  
- No tombstone GC yet (`tombstone_grace_seconds` unused in merge)  
- No compaction IO token bucket  
- CLI `init` stats line may show older default trigger if JSON not updated — check `config/default.json`  
- Root `lsmkv-progress.md` is historical; update or delete when convenient  

---

## 10. Contact / questions for the engine owner

If something fails only under Windows path/locking, or WAL segment cleanup after flush, ask before “fixing” reset policy — it is intentional (reset only when mem + immutables empty).

**Handoff complete when:** partner can clone, test green, explain flush vs compact vs stall from this doc alone.