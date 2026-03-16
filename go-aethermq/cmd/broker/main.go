package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/aethermq/aethermq/internal/broker"
	"github.com/aethermq/aethermq/internal/common"
	"github.com/aethermq/aethermq/internal/config"
	"github.com/aethermq/aethermq/internal/metadata"
	"github.com/aethermq/aethermq/internal/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	configPath = flag.String("config", "", "path to config file")
	nodeID     = flag.String("node-id", "", "unique node identifier")
	listenAddr = flag.String("listen", ":9092", "listen address")
	dataDir    = flag.String("data-dir", "./data", "data directory")
)

func main() {
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := &config.BrokerConfig{
		NodeID:     *nodeID,
		ListenAddr: *listenAddr,
		DataDir:    *dataDir,
	}

	if *configPath != "" {
		loaded, err := config.LoadBrokerConfig(*configPath)
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

	metaStore, err := metadata.NewBoltStore(cfg.DataDir + "/metadata")
	if err != nil {
		logger.Fatal("failed to create metadata store", zap.Error(err))
	}
	defer metaStore.Close()

	segmentStore, err := storage.NewSegmentStore(cfg.DataDir+"/segments", cfg.SegmentSize)
	if err != nil {
		logger.Fatal("failed to create segment store", zap.Error(err))
	}
	defer segmentStore.Close()

	b := broker.NewBroker(cfg, metaStore, segmentStore, logger)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err))
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
	)

	b.RegisterGRPC(grpcServer)

	go func() {
		logger.Info("broker starting", 
			zap.String("node_id", cfg.NodeID),
			zap.String("address", cfg.ListenAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("grpc server error", zap.Error(err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down broker...")
	grpcServer.GracefulStop()
	b.Close()
	logger.Info("broker stopped")
}
