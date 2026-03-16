package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/aethermq/aethermq/internal/config"
	"github.com/aethermq/aethermq/internal/gateway"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	configPath     = flag.String("config", "", "path to config file")
	nodeID         = flag.String("node-id", "", "unique node identifier")
	listenAddr     = flag.String("listen", ":29092", "gateway listen address")
	controllerAddr = flag.String("controller", "127.0.0.1:19091", "controller address")
	metricsAddr    = flag.String("metrics", ":9093", "metrics listen address")
)

func main() {
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := &config.GatewayConfig{
		NodeID:         *nodeID,
		ListenAddr:     *listenAddr,
		ControllerAddr: *controllerAddr,
		MetricsAddr:    *metricsAddr,
	}

	if *configPath != "" {
		loaded, err := config.LoadGatewayConfig(*configPath)
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

	gw := gateway.NewGateway(cfg, logger)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err))
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
	)

	gw.RegisterGRPC(grpcServer)

	go func() {
		logger.Info("gateway starting",
			zap.String("node_id", cfg.NodeID),
			zap.String("address", cfg.ListenAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("grpc server error", zap.Error(err))
		}
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Info("metrics server starting", zap.String("address", cfg.MetricsAddr))
		if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down gateway...")
	grpcServer.GracefulStop()
	gw.Close()
	logger.Info("gateway stopped")
}
