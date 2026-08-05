package main

import (
	"fmt"
	"os"

	"lsmkv/internal/lsm"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "run":
		run()
	case "stats":
		runStats()
	default:
		fmt.Printf("unknown command %q\n", command)
		printUsage()
		os.Exit(1)
	}
}

func run() {
	cfg, err := lsm.LoadConfig("config/default.json")
	if err != nil {
		fmt.Printf("load config error: %v\n", err)
		os.Exit(1)
	}

	store, err := lsm.Open(cfg)
	if err != nil {
		fmt.Printf("open store error: %v\n", err)
		os.Exit(1)
	}
	defer closeStore(store)

	fmt.Println("lsmkv skeleton is alive")
	fmt.Printf("data dir: %s\n", cfg.DataDir)
	fmt.Printf("memtable max bytes: %d\n", cfg.MemtableMaxBytes)
	fmt.Printf("block size: %d\n", cfg.BlockSize)
}

func runStats() {
	cfg, err := lsm.LoadConfig("config/default.json")
	if err != nil {
		fmt.Printf("load config error: %v\n", err)
		os.Exit(1)
	}

	store, err := lsm.Open(cfg)
	if err != nil {
		fmt.Printf("open store error: %v\n", err)
		os.Exit(1)
	}
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

func closeStore(store *lsm.Store) {
	if err := store.Close(); err != nil && err != lsm.ErrStoreClosed {
		fmt.Printf("close store error: %v\n", err)
	}
}

func printUsage() {
	fmt.Println("usage:")
	fmt.Println("  lsmkv run")
	fmt.Println("  lsmkv stats")
}
