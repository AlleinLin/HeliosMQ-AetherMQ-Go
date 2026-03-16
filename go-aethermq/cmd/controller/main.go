package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/aethermq/aethermq/internal/config"
	"github.com/aethermq/aethermq/internal/controller"
	"github.com/aethermq/aethermq/internal/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	configPath = flag.String("config", "", "path to config file")
	nodeID     = flag.String("node-id", "", "unique node identifier")
	listenAddr = flag.String("listen", ":19091", "controller listen address")
	raftAddr   = flag.String("raft", ":19092", "raft listen address")
	dataDir    = flag.String("data-dir", "./data/controller", "data directory")
	joinAddr   = flag.String("join", "", "join existing cluster at address")
)

func main() {
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := &config.ControllerConfig{
		NodeID:     *nodeID,
		ListenAddr: *listenAddr,
		RaftAddr:   *raftAddr,
		DataDir:    *dataDir,
		JoinAddr:   *joinAddr,
	}

	if *configPath != "" {
		loaded, err := config.LoadControllerConfig(*configPath)
		if err != nil {
			logger.Fatal("failed to load config", zap.Error(err))
		}
		cfg = loaded
	}

	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = hostname
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	raftNode, err := raft.NewRaftNode(ctx, cfg.NodeID, cfg.RaftAddr, cfg.DataDir, cfg.JoinAddr)
	if err != nil {
		logger.Fatal("failed to create raft node", zap.Error(err))
	}

	ctrl := controller.NewController(cfg, raftNode, logger)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err))
	}

	grpcServer := grpc.NewServer()
	ctrl.RegisterGRPC(grpcServer)

	go func() {
		logger.Info("controller starting",
			zap.String("node_id", cfg.NodeID),
			zap.String("address", cfg.ListenAddr),
			zap.String("raft_address", cfg.RaftAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("grpc server error", zap.Error(err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down controller...")
	grpcServer.GracefulStop()
	raftNode.Close()
	logger.Info("controller stopped")
}
