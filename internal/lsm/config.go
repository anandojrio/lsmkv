package lsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	DataDir            string  `json:"data_dir"`
	MemtableMaxBytes   int     `json:"memtable_max_bytes"`
	MaxImmutableTables int     `json:"max_immutable_tables"`
	BlockSize          int     `json:"block_size"`
	BloomFalsePositive float64 `json:"bloom_false_positive"`
	WALFsyncEveryN     int     `json:"wal_fsync_every_n"`
	Compression        string  `json:"compression"`
	LogLevel           string  `json:"log_level"`
}

func DefaultConfig() Config {
	return Config{
		DataDir:            "./data",
		MemtableMaxBytes:   67108864,
		MaxImmutableTables: 2,
		BlockSize:          8192,
		BloomFalsePositive: 0.01,
		WALFsyncEveryN:     1,
		Compression:        "off",
		LogLevel:           "info",
	}
}

func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	if len(bytes.TrimSpace(data)) == 0 {
		return cfg, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("%w: data_dir cannot be empty", ErrInvalidArgument)
	}

	if c.MemtableMaxBytes <= 0 {
		return fmt.Errorf("%w: memtable_max_bytes must be > 0", ErrInvalidArgument)
	}

	if c.BlockSize <= 0 {
		return fmt.Errorf("%w: block_size must be > 0", ErrInvalidArgument)
	}

	if c.BloomFalsePositive <= 0 || c.BloomFalsePositive >= 1 {
		return fmt.Errorf("%w: bloom_false_positive must be between 0 and 1", ErrInvalidArgument)
	}

	if c.WALFsyncEveryN <= 0 {
		return fmt.Errorf("%w: wal_fsync_every_n must be > 0", ErrInvalidArgument)
	}

	if c.MaxImmutableTables < 1 {
		return fmt.Errorf("max immutable tables must be at least 1: %w", ErrInvalidArgument)
	}

	switch c.Compression {
	case "off", "snappy", "lz4":
	default:
		return fmt.Errorf("%w: unsupported compression %q", ErrInvalidArgument, c.Compression)
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("%w: unsupported log_level %q", ErrInvalidArgument, c.LogLevel)
	}

	return nil
}
