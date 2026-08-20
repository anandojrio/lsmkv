package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"lsmkv/internal/node"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultAddr        = "127.0.0.1:18080"
	defaultDialTimeout = 2 * time.Second
	defaultRPCTimeout  = 5 * time.Second
)

type flags struct {
	Addr     string
	Key      string
	Value    string
	KeySet   bool
	ValueSet bool
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
	case "put":
		runPut(args)
	case "get":
		runGet(args)
	case "del", "delete":
		runDelete(args)
	default:
		fatalf("unknown command %q", command)
	}
}

func runPut(args []string) {
	f := mustParseFlags(args, true, true)

	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	client, err := node.Dial(ctx, f.Addr, defaultDialTimeout, defaultRPCTimeout)
	if err != nil {
		fatalf("dial error: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Put(ctx, []byte(f.Key), []byte(f.Value)); err != nil {
		fatalf("put error: %v", err)
	}

	fmt.Printf("ok put addr=%s key=%q\n", f.Addr, f.Key)
}

func runGet(args []string) {
	f := mustParseFlags(args, true, false)

	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	client, err := node.Dial(ctx, f.Addr, defaultDialTimeout, defaultRPCTimeout)
	if err != nil {
		fatalf("dial error: %v", err)
	}
	defer func() { _ = client.Close() }()

	value, err := client.Get(ctx, []byte(f.Key))
	if err != nil {
		if status.Code(err) == codes.NotFound {
			fmt.Println("NOT FOUND")
			return
		}
		fatalf("get error: %v", err)
	}

	fmt.Println(string(value))
}

func runDelete(args []string) {
	f := mustParseFlags(args, true, false)

	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	client, err := node.Dial(ctx, f.Addr, defaultDialTimeout, defaultRPCTimeout)
	if err != nil {
		fatalf("dial error: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Delete(ctx, []byte(f.Key)); err != nil {
		fatalf("delete error: %v", err)
	}

	fmt.Printf("ok delete addr=%s key=%q\n", f.Addr, f.Key)
}

func mustParseFlags(args []string, requireKey, requireValue bool) flags {
	f := flags{
		Addr: defaultAddr,
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		switch {
		case arg == "--addr":
			f.Addr = nextFlagValue(args, &i, "--addr")

		case strings.HasPrefix(arg, "--addr="):
			f.Addr = strings.TrimPrefix(arg, "--addr=")
			if f.Addr == "" {
				fatalf("--addr cannot be empty")
			}

		case arg == "--key":
			f.Key = nextFlagValue(args, &i, "--key")
			f.KeySet = true

		case strings.HasPrefix(arg, "--key="):
			f.Key = strings.TrimPrefix(arg, "--key=")
			f.KeySet = true

		case arg == "--value":
			f.Value = nextFlagValue(args, &i, "--value")
			f.ValueSet = true

		case strings.HasPrefix(arg, "--value="):
			f.Value = strings.TrimPrefix(arg, "--value=")
			f.ValueSet = true

		case arg == "--help" || arg == "-h":
			printUsage()
			os.Exit(0)

		case strings.HasPrefix(arg, "--"):
			fatalf("unknown flag %q", arg)

		default:
			fatalf("unexpected positional argument %q", arg)
		}
	}

	if requireKey && !f.KeySet {
		fatalf("command requires --key")
	}

	if requireKey && f.Key == "" {
		fatalf("--key cannot be empty")
	}

	if requireValue && !f.ValueSet {
		fatalf("put requires --value")
	}

	return f
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

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func printUsage() {
	exe := filepath.Base(os.Args[0])

	fmt.Println("usage:")
	fmt.Printf("  %s put --addr HOST:PORT --key K --value V\n", exe)
	fmt.Printf("  %s get --addr HOST:PORT --key K\n", exe)
	fmt.Printf("  %s delete --addr HOST:PORT --key K\n", exe)
	fmt.Printf("  %s help\n", exe)
}
