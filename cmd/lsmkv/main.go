package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"lsmkv/internal/lsm"
)

const defaultConfigPath = "config/default.json"

type commandFlags struct {
	Config string
	Key    string
	Value  string

	KeySet   bool
	ValueSet bool
	Fast     bool
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "help", "--help", "-h":
		printUsage()

	case "init":
		runInit(args)

	case "put":
		runPut(args)

	case "get":
		runGet(args)

	case "del", "delete":
		runDelete(args)

	case "stats":
		runStats(args)

	case "bg-status":
		runBGStatus(args)

	case "flush":
		runFlush(args)

	case "compact":
		runCompact(args)

	case "close":
		runClose(args)

	case "manifest-info":
		runManifestInfo(args)

	case "list-sst":
		runListSST(args)

	case "run":
		run(args)

	default:
		fatalf("unknown command %q", command)
	}
}

func runInit(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, err := lsm.LoadConfig(flags.Config)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fatalf("create data dir error: %v", err)
	}

	store, err := lsm.Open(cfg)
	if err != nil {
		fatalf("open store error: %v", err)
	}
	defer closeStore(store)

	fmt.Println("config loaded ✓")
	fmt.Printf("data dir ready ✓ (%s)\n", cfg.DataDir)
	fmt.Printf("wal dir ready ✓ (%s)\n", filepath.Join(cfg.DataDir, "wal"))
	fmt.Println("store opened ✓")
	fmt.Printf(
		"memtable_max_bytes=%d block_size=%d wal_fsync_every_n=%d wal_segment_roll_bytes=%d l0_compaction_trigger=%d\n",
		cfg.MemtableMaxBytes,
		cfg.BlockSize,
		cfg.WALFsyncEveryN,
		cfg.WALSegmentRollBytes,
		cfg.L0CompactionTrigger,
	)
}

func runPut(args []string) {
	flags := mustParseFlags(args, true, true)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	if err := store.Put([]byte(flags.Key), []byte(flags.Value)); err != nil {
		fatalf("put error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf(
		"ok put key=%q seqno=%d memtable_bytes=%d\n",
		flags.Key,
		stats.LastSeqNo,
		stats.ActiveBytes,
	)
}

func runGet(args []string) {
	flags := mustParseFlags(args, true, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	value, found, err := store.Get([]byte(flags.Key))
	if err != nil {
		fatalf("get error: %v", err)
	}

	if !found {
		fmt.Println("NOT FOUND")
		return
	}

	fmt.Println(string(value))
}

func runDelete(args []string) {
	flags := mustParseFlags(args, true, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	if err := store.Delete([]byte(flags.Key)); err != nil {
		fatalf("delete error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf(
		"ok del key=%q seqno=%d memtable_bytes=%d\n",
		flags.Key,
		stats.LastSeqNo,
		stats.ActiveBytes,
	)
}

func runStats(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	stats := store.Stats()
	m := stats.Metrics

	fmt.Println("=== store stats ===")
	fmt.Printf("engine_status:      %s\n", stats.EngineStatus)
	fmt.Printf("last_seqno:         %d\n", stats.LastSeqNo)
	fmt.Println("")
	fmt.Println("--- wal ---")
	fmt.Printf("active_segment_id:  %d\n", stats.ActiveSegmentID)
	fmt.Printf("total_wal_segments: %d\n", stats.TotalWALSegments)
	fmt.Printf("bytes_written:      %d\n", stats.BytesWritten)
	fmt.Println("")
	fmt.Println("--- memtable ---")
	fmt.Printf("active_entries:     %d\n", stats.ActiveEntries)
	fmt.Printf("active_bytes:       %d\n", stats.ActiveBytes)
	fmt.Printf("immutables_count:   %d\n", stats.ImmutablesCount)
	fmt.Printf("immutables_bytes:   %d\n", stats.ImmutablesBytes)
	fmt.Println("")
	fmt.Println("--- sstables ---")
	fmt.Printf("sst_count:          %d\n", stats.SSTCount)
	fmt.Printf("sst_total_bytes:    %d\n", stats.SSTTotalBytes)
	fmt.Println("")
	fmt.Println("--- metrics (unit 8) ---")
	fmt.Printf("puts_total:               %d\n", m.PutsTotal)
	fmt.Printf("deletes_total:            %d\n", m.DeletesTotal)
	fmt.Printf("bloom_checks_total:       %d\n", m.BloomChecksTotal)
	fmt.Printf("bloom_skips_total:        %d\n", m.BloomSkipsTotal)
	fmt.Printf("block_reads_total:        %d\n", m.BlockReadsTotal)
	fmt.Printf("flushes_total:            %d\n", m.FlushesTotal)
	fmt.Printf("compactions_total:        %d\n", m.CompactionsTotal)
	fmt.Printf("last_flush_duration_ms:   %d\n", m.LastFlushDurationMs)
	fmt.Printf("last_compact_duration_ms: %d\n", m.LastCompactDurationMs)
}

func runBGStatus(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	bg := store.BGStatus()
	fmt.Println("background status")
	fmt.Printf("flush_running: %v\n", bg.FlushRunning)
	fmt.Printf("compact_running: %v\n", bg.CompactRunning)
	fmt.Printf("flush_queue_len: %d\n", bg.FlushQueueLen)
	fmt.Printf("compact_queue_len: %d\n", bg.CompactQueueLen)
	fmt.Printf("flush_jobs_total: %d\n", bg.FlushJobsTotal)
	fmt.Printf("compact_jobs_total: %d\n", bg.CompactJobsTotal)
	fmt.Printf("last_flush_ms: %d\n", bg.LastFlushMs)
	fmt.Printf("last_compact_ms: %d\n", bg.LastCompactMs)
	fmt.Printf("compaction_trigger: %d\n", bg.CompactionTrigger)
	if bg.LastError != "" {
		fmt.Printf("last_error: %s\n", bg.LastError)
	} else {
		fmt.Println("last_error: none")
	}
}

func runFlush(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	if err := store.ForceFlush(); err != nil {
		fatalf("flush error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf(
		"ok flush immutables_remaining=%d sst_count=%d last_seqno=%d\n",
		stats.ImmutablesCount,
		stats.SSTCount,
		stats.LastSeqNo,
	)
}

func runCompact(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	before := store.Stats()

	if err := store.Compact(); err != nil {
		fatalf("compact error: %v", err)
	}

	after := store.Stats()
	fmt.Printf(
		"ok compact sst_count_before=%d sst_count_after=%d sst_total_bytes=%d\n",
		before.SSTCount,
		after.SSTCount,
		after.SSTTotalBytes,
	)
}

func runClose(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)

	var err error
	if flags.Fast {
		err = store.CloseFast()
	} else {
		err = store.CloseGraceful()
	}
	if err != nil {
		fatalf("close error: %v", err)
	}

	if flags.Fast {
		fmt.Println("closed fast ✓")
	} else {
		fmt.Println("closed ✓")
	}
}

// runManifestInfo čita manifest JSON direktno — bez otvaranja Store-a.
// Ispisuje listu svih SST-ova sa metapodacima.
func runManifestInfo(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, err := lsm.LoadConfig(flags.Config)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	manifestPath := filepath.Join(cfg.DataDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		fatalf("read manifest error: %v", err)
	}

	var raw struct {
		Epoch  uint64 `json:"epoch"`
		Tables []struct {
			ID       uint64 `json:"id"`
			File     string `json:"file"`
			MinKey   string `json:"min_key"`
			MaxKey   string `json:"max_key"`
			MinSeqNo uint64 `json:"min_seq_no"`
			MaxSeqNo uint64 `json:"max_seq_no"`
			FileSize int64  `json:"file_size"`
		} `json:"tables"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		fatalf("parse manifest error: %v", err)
	}

	fmt.Printf("=== manifest info (epoch %d) ===\n", raw.Epoch)
	fmt.Printf("sst count: %d\n\n", len(raw.Tables))

	if len(raw.Tables) == 0 {
		fmt.Println("(no sstables)")
		return
	}

	fmt.Printf("%-8s %-16s %-20s %-20s %-10s %-10s %-12s\n",
		"id", "file", "min_key", "max_key", "min_seq", "max_seq", "size_bytes")
	fmt.Println(strings.Repeat("-", 100))

	for _, t := range raw.Tables {
		minKey := t.MinKey
		if len(minKey) > 18 {
			minKey = minKey[:15] + "..."
		}
		maxKey := t.MaxKey
		if len(maxKey) > 18 {
			maxKey = maxKey[:15] + "..."
		}
		fmt.Printf("%-8d %-16s %-20s %-20s %-10d %-10d %-12d\n",
			t.ID, t.File, minKey, maxKey, t.MinSeqNo, t.MaxSeqNo, t.FileSize)
	}
}

// runListSST otvara svaki SST fajl i ispisuje detalje:
// broj entries, seq range, bloom bits, veličinu.
func runListSST(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, err := lsm.LoadConfig(flags.Config)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	manifestPath := filepath.Join(cfg.DataDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		fatalf("read manifest error: %v", err)
	}

	var raw struct {
		Epoch  uint64 `json:"epoch"`
		Tables []struct {
			ID       uint64 `json:"id"`
			File     string `json:"file"`
			MinSeqNo uint64 `json:"min_seq_no"`
			MaxSeqNo uint64 `json:"max_seq_no"`
			FileSize int64  `json:"file_size"`
		} `json:"tables"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		fatalf("parse manifest error: %v", err)
	}

	fmt.Printf("=== list-sst (epoch=%d, count=%d) ===\n\n", raw.Epoch, len(raw.Tables))

	if len(raw.Tables) == 0 {
		fmt.Println("(no sstables)")
		return
	}

	fmt.Printf("%-8s %-16s %-10s %-10s %-10s %-12s\n",
		"id", "file", "entries", "min_seq", "max_seq", "size_bytes")
	fmt.Println(strings.Repeat("-", 76))

	for _, t := range raw.Tables {
		sstPath := filepath.Join(cfg.DataDir, t.File)
		entryCount := countSSTEntries(sstPath)
		fmt.Printf("%-8d %-16s %-10d %-10d %-10d %-12d\n",
			t.ID, t.File, entryCount, t.MinSeqNo, t.MaxSeqNo, t.FileSize)
	}
}

// countSSTEntries otvara SST reader i broji sve entries.
// Vraća -1 ako fajl nije čitljiv.
func countSSTEntries(path string) int {
	r, err := lsm.OpenSSTableReader(path)
	if err != nil {
		return -1
	}
	defer r.Close()

	entries, err := r.AllEntries()
	if err != nil {
		return -1
	}
	return len(entries)
}

func run(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, store := loadAndOpenStore(flags.Config)
	defer closeStore(store)

	fmt.Println("lsmkv skeleton is alive")
	fmt.Printf("data dir: %s\n", cfg.DataDir)
	fmt.Printf("memtable max bytes: %d\n", cfg.MemtableMaxBytes)
	fmt.Printf("block size: %d\n", cfg.BlockSize)
}

func mustParseFlags(args []string, requireKey, requireValue bool) commandFlags {
	flags := commandFlags{
		Config: defaultConfigPath,
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		switch {
		case arg == "--config":
			flags.Config = nextFlagValue(args, &i, "--config")

		case strings.HasPrefix(arg, "--config="):
			flags.Config = strings.TrimPrefix(arg, "--config=")
			if flags.Config == "" {
				fatalf("--config cannot be empty")
			}

		case arg == "--key":
			flags.Key = nextFlagValue(args, &i, "--key")
			flags.KeySet = true

		case strings.HasPrefix(arg, "--key="):
			flags.Key = strings.TrimPrefix(arg, "--key=")
			flags.KeySet = true

		case arg == "--value":
			flags.Value = nextFlagValue(args, &i, "--value")
			flags.ValueSet = true

		case strings.HasPrefix(arg, "--value="):
			flags.Value = strings.TrimPrefix(arg, "--value=")
			flags.ValueSet = true

		case arg == "--fast" || arg == "--fast=true":
			flags.Fast = true

		case arg == "--fast=false":
			flags.Fast = false

		case arg == "--help" || arg == "-h":
			printUsage()
			os.Exit(0)

		case strings.HasPrefix(arg, "--"):
			fatalf("unknown flag %q", arg)

		default:
			fatalf("unexpected positional argument %q", arg)
		}
	}

	if requireKey && !flags.KeySet {
		fatalf("command requires --key")
	}

	if requireKey && flags.Key == "" {
		fatalf("--key cannot be empty")
	}

	if requireValue && !flags.ValueSet {
		fatalf("put requires --value")
	}

	return flags
}

func nextFlagValue(args []string, index *int, name string) string {
	nextIndex := *index + 1

	if nextIndex >= len(args) {
		fatalf("%s requires a value", name)
	}

	value := args[nextIndex]
	if strings.HasPrefix(value, "--") {
		fatalf("%s requires a value", name)
	}

	*index = nextIndex
	return value
}

func mustOpenStore(configPath string) *lsm.Store {
	_, store := loadAndOpenStore(configPath)
	return store
}

func loadAndOpenStore(configPath string) (lsm.Config, *lsm.Store) {
	cfg, err := lsm.LoadConfig(configPath)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fatalf("create data dir error: %v", err)
	}

	store, err := lsm.Open(cfg)
	if err != nil {
		fatalf("open store error: %v", err)
	}

	return cfg, store
}

func closeStore(store *lsm.Store) {
	if store == nil {
		return
	}

	if err := store.Close(); err != nil && err != lsm.ErrStoreClosed {
		fmt.Fprintf(os.Stderr, "close store error: %v\n", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func printUsage() {
	exe := filepath.Base(os.Args[0])

	fmt.Println("usage:")
	fmt.Printf("  %s init [--config path]\n", exe)
	fmt.Printf("  %s put --key K --value V [--config path]\n", exe)
	fmt.Printf("  %s get --key K [--config path]\n", exe)
	fmt.Printf("  %s del --key K [--config path]\n", exe)
	fmt.Printf("  %s stats [--config path]\n", exe)
	fmt.Printf("  %s bg-status [--config path]\n", exe)
	fmt.Printf("  %s flush [--config path]\n", exe)
	fmt.Printf("  %s compact [--config path]\n", exe)
	fmt.Printf("  %s close [--fast] [--config path]\n", exe)
	fmt.Printf("  %s manifest-info [--config path]\n", exe)
	fmt.Printf("  %s list-sst [--config path]\n", exe)
	fmt.Printf("  %s run [--config path] (legacy smoke test)\n", exe)
	fmt.Printf("  %s help\n", exe)
}
