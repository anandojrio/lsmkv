# Project Description — lsmkv

## What This Project Is

`lsmkv` is a **Dynamo-style distributed key-value database** built on an **LSM (Log-Structured Merge) storage engine**. The project is split into two phases:

- **Phase A**: a single-node storage engine that is crash-safe, observable, and tested. This is the primary deliverable and the focus of the implementation plan.
- **Phase B**: distribution features inspired by Amazon's Dynamo — consistent hashing, replication, read/write quorums, hinted handoff, anti-entropy, and conflict resolution. Treated as a stretch goal beyond Phase A.

An LSM engine is the storage technique behind real production databases such as Cassandra, RocksDB, and LevelDB. Writes are appended to a log and staged in memory before being flushed to sorted, immutable files on disk, which are later merged (compacted) to stay efficient. This design favors very high write throughput at the cost of more complex read and maintenance logic — which is exactly what Phase A of this project is testing.

## Why LSM First

LSM engines are chosen as the foundation because they give:

- **Durability** — no acknowledged write is lost on crash
- **Good write performance** — sequential disk appends instead of random writes
- **Predictable reads** — Bloom filters and sparse indexes avoid unnecessary disk scans
- **A clean path to compaction** — merging files and dropping stale/deleted data over time

Dynamo-style distribution (Phase B) depends on semantics that only make sense once Phase A works correctly — especially tombstones (deletion markers), version numbers, and "newest write wins" conflict resolution.

## Selected Language: Go

Go was chosen over Java, Rust, and C++ (all valid options per the project spec) for the following reasons:

- **Concurrency model fits the architecture directly.** Go's goroutines and channels are a natural match for the writer/reader/background-worker pattern the project requires (single writer thread, background flush and compaction workers, bounded queues for backpressure).
- **Simplicity and short learning curve.** Go has a small, consistent syntax with one enforced formatting style (`gofmt`), making it easier for two people to stay aligned on code style with no configuration.
- **Strong precedent for this exact domain.** Go is the language behind etcd, CockroachDB, and InfluxDB — all real-world distributed storage systems built on the same principles this project teaches.
- **Built-in tooling for correctness testing.** Go ships with a native fuzz tester (`go test -fuzz`), which maps directly onto the project's fault-injection and crash-drill testing requirements with no extra dependencies.
- **Trade-off accepted:** Go has no built-in sorted map (unlike Java's `TreeMap`), so the Memtable's ordered structure must be built or assembled from existing primitives (sorted slice with binary search insert, or a simple balanced tree).

## Core Technologies and Libraries

| Concern | Technology | Notes |
|---|---|---|
| Language | Go 1.23+ | Single toolchain, no separate build system |
| Module/dependency management | `go.mod` / `go mod` | Built into the toolchain |
| Binary file I/O | `encoding/binary`, `os`, `bufio` | For WAL records and SSTable blocks |
| Checksums | `hash/crc32` (standard library) | Detects partial/corrupted writes |
| Atomic file operations | `os.Rename`, `file.Sync()` | Crash-safe publication of new files |
| Concurrency | Goroutines, channels, `sync.RWMutex`, `sync/atomic` | Writer/reader coordination, backpressure, safe pointer swaps |
| Config format | JSON (`encoding/json`, standard library) | Human-readable, no extra dependency |
| CLI | Standard library `flag`/manual dispatch, or `cobra` | Keeps subcommand parsing simple |
| Testing | `testing` package, `go test -fuzz` | Unit tests and fault injection |
| Logging/metrics | `log/slog` (structured logging, standard library) | Observability requirement in Section 8 |
| Version control | Git + GitHub | Shared repo, feature branches, pull requests |

No web framework, database driver, or ORM is used anywhere in this project — the entire storage engine is built from first principles using only the Go standard library, which is intentional: the goal is to understand how databases work internally, not to integrate existing ones.

## What Gets Built, In Order (Phase A)

1. **Project skeleton & API** — repo layout, CLI stub commands, config loader, plain-English API contract.
2. **WAL (Write-Ahead Log) & crash recovery** — durable append-before-acknowledge logging, checksummed records, replay-on-restart.
3. **Memtable** — in-memory sorted table of recent writes, with rotation to an immutable snapshot when full.
4. **SSTable writer** — flushes immutable memtables into sorted, immutable on-disk files with data blocks, a sparse index, and a Bloom filter.
5. **SSTable reader** — enables real disk reads: Bloom filter skip, binary search over the index, block scan, tombstone-aware lookup.
6. **Manifest & versioning** — a durable list of which files exist, and an immutable in-memory snapshot so readers never see a half-updated file set.
7. **Compaction** — merges multiple small SSTables into fewer, larger ones, dropping obsolete versions and resolving tombstones.
8. **Concurrency & scheduling** — formal single-writer/many-reader rules, background worker queues, backpressure, and clean shutdown.
9. **Observability & tooling** — structured logs, metrics, CLI/HTTP inspection commands.
10. **Testing & fault injection** — unit tests, property-based tests, and repeatable crash/power-cut simulations.

## What "Done" Looks Like for Phase A

- No acknowledged write is ever lost after a simulated crash.
- The newest value for a key always wins; deletions (tombstones) correctly hide older values.
- Absent keys are cheap to check (Bloom filters skip most disk access); present keys touch at most one or two disk blocks.
- New files and their metadata become visible all at once — never partially.
- Background compaction keeps the number of files from growing without bound.
- The system can explain its own internal state through a single `stats` command.
- Test failures are reproducible: they print a seed and a short "how to repro" note.

## Stretch Goals (Optional, Time Permitting)

- **Phase B**: consistent hashing ring, replication factor, read/write quorums, hinted handoff, anti-entropy repair, conflict resolution (vector clocks or last-write-wins).
- Leveled compaction (capping read amplification further than simple size-tiered compaction).
- Prefix/partitioned Bloom filters, range iterators, TTL-based expiry.

These are explicitly lower priority than making Phase A "boringly reliable" first.
