# lsmkv — Project status (Phase A handoff)

**Date:** 2026-08-12  
**Owner (this slice):** Matija — Units 0–7 (engine core through compaction + scheduling)  
**Repo:** https://github.com/anandojrio/lsmkv  
**Verify:** `go test ./internal/lsm -count=1` and `go test ./... -count=1` must stay green.

Ground truth is the code + tests. Older notes (e.g. root `lsmkv-progress.md`) may be stale — prefer this file and `docs/HANDOFF.md`.

---

## Phase overview

| Phase | Scope | Status |
|-------|--------|--------|
| **Phase A** | Single-node LSM (spec Sections 0–9) | **0–7 done**; **8–9 remaining** |
| **Phase B** | Dynamo-style distribution | **Not started** (partner) |

---

## Unit checklist

### Done — your delivery

| Unit | Name | Status | Notes |
|------|------|--------|--------|
| 0 | Skeleton, config, CLI | **Done** | `init/put/get/del/stats/flush/compact/close/bg-status` |
| 1 | WAL & recovery | **Done** | Segmented WAL, checksums, tail truncate, reset after safe flush |
| 2 | Memtable | **Done** | Active + immutables, rotation, `ErrTooManyImmutables` |
| 3 | SSTable writer | **Done** | Blocks, index, Bloom, temp+rename |
| 4 | SSTable reader + Get | **Done** | Bloom→index→block; mem → immutables → Version |
| 5 | Manifest & Version | **Done** | Atomic manifest; immutable Version publish on flush/compact |
| 6 | Compaction (size-tiered) | **Done** | Picker (fan-in + size ratio + newest fallback); N-way merge; newest seq wins; **tombstones always kept** |
| 7 | Concurrency & scheduling | **Done** | Flush + compact workers; flush priority; L0 trigger auto-compact; `L0StopWrites` → `ErrWriteStall`; graceful/fast close; `BGStatus` |

### Not done — partner (Phase A finish)

| Unit | Name | Status |
|------|------|--------|
| 8 | Observability | **Not started** — rich metrics, structured logs, `manifest-info` / `list-sst` / doctor-style tooling |
| 9 | Fault injection & chaos | **Not started** — crash points, scripted kill/reopen, `docs/chaos.md`, CI outline |

### Explicitly out of this slice (optional / later)

- Compaction IO throttle (`compaction_io_mb_per_s`)
- Multi concurrent compactors (`compaction_max_concurrent` > 1)
- Tombstone drop by `tombstone_grace_seconds` (knob exists; **policy = never drop yet**)
- Block cache, leveled compaction, HTTP admin

---

## What changed in the final 6+7 polish (track for PR)

1. **Size-tiered picker** — `pickSizeTiered` / `newCompactionPlan(manifest, cfg)` (no longer “two oldest only”).
2. **N-input compaction** — `plan.Inputs` length 2..K; `compactReaders` + `mergeEntries` already multi-set.
3. **Config** — `size_tiered_fan_in`, `size_tiered_size_ratio`, `tombstone_grace_seconds`, `l0_compaction_trigger`, `l0_stop_writes`.
4. **`ErrWriteStall`** — Put/Delete blocked when live SST count ≥ `l0_stop_writes` (0 = off).
5. **Scheduler** — compact worker skips while immutables pending; `maybeEnqueueCompact` same; re-enqueue flush/compact.
6. **`Store.Compact`** — no-op if immutables > 0 (flush first).
7. **Tests** — picker, merge tombstone retain, four-table compact, write stall, existing flush/close/bg tests.
8. **Tombstones** — kept forever in compaction output (Phase-B friendly).

---

## How to verify

```text
go build ./...
go test ./internal/lsm -count=1
go test ./... -count=1
go build -o lsmkv.exe ./cmd/lsmkv
```

CLI smoke (prefer empty data dir):

```text
lsmkv init
lsmkv put --key hello --value world
lsmkv get --key hello
lsmkv flush
lsmkv compact
lsmkv bg-status
lsmkv stats
lsmkv close
```

**Last verified:** 2026-08-12 — `go test ./internal/lsm` ok; CLI flush/compact/bg-status/stats/close ok.

---

## Invariants partners must not break

1. **Acked write durability** — WAL append (+ fsync policy) before memtable update; recovery rebuilds memtable.
2. **Read order** — active mem → immutables newest-first → SSTables newest-first; first hit wins; tombstone ⇒ not found.
3. **Publish order** — SST temp+rename **before** manifest save; delete old SST **only after** new manifest durable.
4. **Version immutability** — never mutate live Version in place; replace pointer after publish.
5. **Tombstones retained** in compaction until Phase B policy is deliberately changed.
6. **Single writer** — Put/Delete under write lock; Get under read lock / Version snapshot.
7. **Flush > compact** — do not compact while immutables are queued.

---

## Suggested partner order

1. Unit 8 — observability (makes demos/grading easy).  
2. Unit 9 — chaos / crash injection.  
3. Phase B — distribution (see HANDOFF).