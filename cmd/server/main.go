package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"lsmkv/internal/lsm"
	"lsmkv/internal/node"
	lsmkvv1 "lsmkv/proto"

	"google.golang.org/grpc"
)

const (
	defaultConfigPath   = "config/node-default.json"
	defaultVirtualNodes = 32
)

func main() {
	addr := flag.String("addr", "", "gRPC listen address override")
	configPath := flag.String("config", defaultConfigPath, "path to JSON config file")
	flag.Parse()

	cfg, err := node.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load node config error: %v", err)
	}

	if *addr != "" {
		cfg.ListenAddr = *addr
	}

	rt, err := node.NewRuntime(cfg, defaultVirtualNodes)
	if err != nil {
		log.Fatalf("build runtime error: %v", err)
	}

	if err := os.MkdirAll(rt.Config.Storage.DataDir, 0o755); err != nil {
		log.Fatalf("create data dir error: %v", err)
	}

	store, err := lsm.Open(rt.Config.Storage)
	if err != nil {
		log.Fatalf("open store error: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil && !errors.Is(err, lsm.ErrStoreClosed) {
			log.Printf("close store error: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", rt.Config.ListenAddr)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}

	grpcServer := grpc.NewServer()
	lsmkvv1.RegisterKVServiceServer(grpcServer, node.NewServer(store, rt))

	errCh := make(chan error, 1)
	go func() {
		log.Printf(
			"node_id=%s listen_addr=%s replication_factor=%d cluster_nodes=%d",
			rt.Config.NodeID,
			rt.Config.ListenAddr,
			rt.ReplicationFactor,
			len(rt.ClusterNodes),
		)
		errCh <- grpcServer.Serve(lis)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("grpc serve error: %v", err)
		}
	case <-ctx.Done():
		log.Println("shutdown signal received")

		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			log.Println("gRPC server stopped gracefully")
		case <-time.After(5 * time.Second):
			log.Println("graceful stop timed out; forcing stop")
			grpcServer.Stop()
		}
	}
}
