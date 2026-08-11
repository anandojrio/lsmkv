package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"lsmkv/internal/lsm"
)

const defaultConfigPath = "config/default.json"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
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
	case "flush":
		runFlush(args)
	case "compact":
		runCompact(args)
	case "close":
		runClose(args)
	case "run":
		// Kept for backward compatibility with the old skeleton command.
		run(args)
	default:
		fmt.Printf("unknown command %q\n", command)
		printUsage()
		os.Exit(1)
	}
}

func runInit(args []string) {
	cfgPath := flagValue(args, "config", defaultConfigPath)

	cfg, err := lsm.LoadConfig(cfgPath)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fatalf("create data dir error: %v", err)
	}

	// Opening the store creates/loads WAL + manifest machinery under data_dir.
	store, err := lsm.Open(cfg)
	if err != nil {
		fatalf("open store error: %v", err)
	}
	defer closeStore(store)

	fmt.Println("config loaded ✓")
	fmt.Printf("data dir ready ✓ (%s)\n", cfg.DataDir)
	fmt.Println("manifest placeholder ✓")
	fmt.Printf("memtable_max_bytes=%d block_size=%d wal_fsync_every_n=%d\n",
		cfg.MemtableMaxBytes, cfg.BlockSize, cfg.WALFsyncEveryN)
}

func runPut(args []string) {
	key, ok := requireFlag(args, "key")
	if !ok {
		fatalf("put requires --key")
	}
	value, ok := requireFlag(args, "value")
	if !ok {
		// Empty value is allowed by the engine contract; missing flag is not.
		// Treat "--value" with no following token as empty string if present as bare form.
		if hasFlag(args, "value") {
			value = ""
		} else {
			fatalf("put requires --value")
		}
	}

	store := mustOpenStore(args)
	defer closeStore(store)

	if err := store.Put([]byte(key), []byte(value)); err != nil {
		fatalf("put error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf("ok put key=%q seqno=%d memtable_bytes=%d\n", key, stats.LastSeqNo, stats.ActiveBytes)
}

func runGet(args []string) {
	key, ok := requireFlag(args, "key")
	if !ok {
		fatalf("get requires --key")
	}

	store := mustOpenStore(args)
	defer closeStore(store)

	value, found, err := store.Get([]byte(key))
	if err != nil {
		fatalf("get error: %v", err)
	}
	if !found {
		fmt.Println("NOT FOUND")
		return
	}
	fmt.Printf("%s\n", string(value))
}

func runDelete(args []string) {
	key, ok := requireFlag(args, "key")
	if !ok {
		fatalf("del requires --key")
	}

	store := mustOpenStore(args)
	defer closeStore(store)

	if err := store.Delete([]byte(key)); err != nil {
		fatalf("delete error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf("ok del key=%q seqno=%d memtable_bytes=%d\n", key, stats.LastSeqNo, stats.ActiveBytes)
}

func runStats(args []string) {
	store := mustOpenStore(args)
	defer closeStore(store)

	stats := store.Stats()

	fmt.Println("store stats")
	fmt.Printf("engine status: %s\n", stats.EngineStatus)
	fmt.Printf("active segment id: %d\n", stats.ActiveSegmentID)
	fmt.Printf("bytes written: %d\n", stats.BytesWritten)
	fmt.Printf("total wal segments: %d\n", stats.TotalWALSegments)
	fmt.Printf("last seqno: %d\n", stats.LastSeqNo)
	fmt.Printf("active entries: %d\n", stats.ActiveEntries)
	fmt.Printf("active bytes: %d\n", stats.ActiveBytes)
	fmt.Printf("immutables count: %d\n", stats.ImmutablesCount)
	fmt.Printf("immutables bytes: %d\n", stats.ImmutablesBytes)
	fmt.Printf("sst count: %d\n", stats.SSTCount)
	fmt.Printf("sst total bytes: %d\n", stats.SSTTotalBytes)
}

func runFlush(args []string) {
	store := mustOpenStore(args)
	defer closeStore(store)

	// Rotate the active memtable first so FlushAll has something to drain
	// when the active table is non-empty but under the size threshold.
	// We do that by writing nothing special: FlushAll only drains immutables.
	// So for an operator "flush now", freeze active if it has data via a
	// tiny helper path: put is not appropriate. Call FlushAll only.
	//
	// If you need "force rotate active", that belongs in the engine later.
	if err := store.FlushAll(); err != nil {
		fatalf("flush error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf("ok flush immutables_remaining=%d sst_count=%d\n",
		stats.ImmutablesCount, stats.SSTCount)
}

func runCompact(args []string) {
	store := mustOpenStore(args)
	defer closeStore(store)

	if err := store.Compact(); err != nil {
		fatalf("compact error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf("ok compact sst_count=%d sst_total_bytes=%d\n",
		stats.SSTCount, stats.SSTTotalBytes)
}

func runClose(args []string) {
	// CLI close means: open cleanly, close cleanly, report success.
	// Useful as a smoke test that the store can shut down without error.
	store := mustOpenStore(args)
	if err := store.Close(); err != nil {
		fatalf("close error: %v", err)
	}
	fmt.Println("closed ✓")
}

func run(args []string) {
	// Legacy skeleton command — kept so old scripts still work.
	cfg, store := mustLoadAndOpen(args)
	defer closeStore(store)

	fmt.Println("lsmkv skeleton is alive")
	fmt.Printf("data dir: %s\n", cfg.DataDir)
	fmt.Printf("memtable max bytes: %d\n", cfg.MemtableMaxBytes)
	fmt.Printf("block size: %d\n", cfg.BlockSize)
}

// --- helpers ---

func mustOpenStore(args []string) *lsm.Store {
	_, store := mustLoadAndOpen(args)
	return store
}

func mustLoadAndOpen(args []string) (lsm.Config, *lsm.Store) {
	cfgPath := flagValue(args, "config", defaultConfigPath)

	cfg, err := lsm.LoadConfig(cfgPath)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	// Ensure parent of data dir exists when people use relative paths.
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
		fmt.Printf("close store error: %v\n", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// flagValue returns the value of --name, or def if missing.
// Supports: --name VALUE  and  --name=VALUE
func flagValue(args []string, name, def string) string {
	long := "--" + name
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == long {
			if i+1 >= len(args) {
				return def
			}
			return args[i+1]
		}
		if strings.HasPrefix(a, long+"=") {
			return strings.TrimPrefix(a, long+"=")
		}
	}
	return def
}

// requireFlag is like flagValue but reports whether the flag was present.
func requireFlag(args []string, name string) (string, bool) {
	long := "--" + name
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == long {
			if i+1 >= len(args) {
				return "", true // present but empty / missing token
			}
			// Next token starting with -- means empty value was intended only if name is value.
			if strings.HasPrefix(args[i+1], "--") {
				return "", true
			}
			return args[i+1], true
		}
		if strings.HasPrefix(a, long+"=") {
			return strings.TrimPrefix(a, long+"="), true
		}
	}
	return "", false
}

func hasFlag(args []string, name string) bool {
	_, ok := requireFlag(args, name)
	return ok
}

func printUsage() {
	exe := filepath.Base(os.Args[0])
	fmt.Println("usage:")
	fmt.Printf("  %s init [--config path]\n", exe)
	fmt.Printf("  %s put  --key K --value V [--config path]\n", exe)
	fmt.Printf("  %s get  --key K [--config path]\n", exe)
	fmt.Printf("  %s del  --key K [--config path]\n", exe)
	fmt.Printf("  %s stats [--config path]\n", exe)
	fmt.Printf("  %s flush [--config path]\n", exe)
	fmt.Printf("  %s compact [--config path]\n", exe)
	fmt.Printf("  %s close [--config path]\n", exe)
	fmt.Printf("  %s run [--config path]   (legacy smoke test)\n", exe)
}
