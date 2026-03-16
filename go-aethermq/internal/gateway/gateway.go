package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/aethermq/aethermq/internal/common"
	"github.com/aethermq/aethermq/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	connectionCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aethermq_gateway_connections",
		Help: "Number of active connections",
	})

	requestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aethermq_gateway_requests_total",
		Help: "Total number of requests processed",
	}, []string{"method", "topic"})
)

type Gateway struct {
	cfg         *config.GatewayConfig
	logger      *zap.Logger
	brokerPool  *BrokerPool
	router      *Router
	rateLimiter *RateLimiter
	mu          sync.RWMutex
	running     bool
}

type BrokerPool struct {
	mu      sync.RWMutex
	brokers map[string]*BrokerConn
}

type BrokerConn struct {
	brokerID string
	address  string
	conn     *grpc.ClientConn
	healthy  bool
	lastHB   time.Time
}

type Router struct {
	mu          sync.RWMutex
	topicRoutes map[string]*TopicRoute
}

type TopicRoute struct {
	topic      string
	partitions map[int32]*PartitionRoute
	version    int64
}

type PartitionRoute struct {
	partition int32
	leader    string
	replicas  []string
}

type RateLimiter struct {
	mu       sync.Mutex
	limits   map[string]*TokenBucket
}

type TokenBucket struct {
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func NewGateway(cfg *config.GatewayConfig, logger *zap.Logger) *Gateway {
	gw := &Gateway{
		cfg:        cfg,
		logger:     logger,
		brokerPool: NewBrokerPool(),
		router:     NewRouter(),
		rateLimiter: NewRateLimiter(),
	}
	go gw.healthCheck()
	return gw
}

func NewBrokerPool() *BrokerPool {
	return &BrokerPool{
		brokers: make(map[string]*BrokerConn),
	}
}

func (p *BrokerPool) AddBroker(brokerID, address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.brokers[brokerID]; ok {
		return nil
	}

	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	p.brokers[brokerID] = &BrokerConn{
		brokerID: brokerID,
		address:  address,
		conn:     conn,
		healthy:  true,
		lastHB:   time.Now(),
	}
	return nil
}

func (p *BrokerPool) RemoveBroker(brokerID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.brokers[brokerID]; ok {
		conn.conn.Close()
		delete(p.brokers, brokerID)
	}
}

func (p *BrokerPool) GetBroker(brokerID string) (*BrokerConn, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	conn, ok := p.brokers[brokerID]
	return conn, ok
}

func (p *BrokerPool) ListBrokers() []*BrokerConn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var brokers []*BrokerConn
	for _, b := range p.brokers {
		brokers = append(brokers, b)
	}
	return brokers
}

func NewRouter() *Router {
	return &Router{
		topicRoutes: make(map[string]*TopicRoute),
	}
}

func (r *Router) UpdateRoute(topic string, partitions map[int32]*PartitionRoute) {
	r.mu.Lock()
	defer r.mu.Unlock()

	route := &TopicRoute{
		topic:      topic,
		partitions: partitions,
		version:    time.Now().UnixNano(),
	}
	r.topicRoutes[topic] = route
}

func (r *Router) GetRoute(topic string, partition int32) (*PartitionRoute, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	route, ok := r.topicRoutes[topic]
	if !ok {
		return nil, false
	}

	pr, ok := route.partitions[partition]
	return pr, ok
}

func (r *Router) GetLeader(topic string, partition int32) (string, bool) {
	pr, ok := r.GetRoute(topic, partition)
	if !ok {
		return "", false
	}
	return pr.leader, true
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		limits: make(map[string]*TokenBucket),
	}
}

func (rl *RateLimiter) Allow(key string, rate float64) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, ok := rl.limits[key]
	if !ok {
		bucket = &TokenBucket{
			tokens:     rate,
			maxTokens:  rate * 10,
			refillRate: rate,
			lastRefill: time.Now(),
		}
		rl.limits[key] = bucket
	}

	now := time.Now()
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * bucket.refillRate
	if bucket.tokens > bucket.maxTokens {
		bucket.tokens = bucket.maxTokens
	}
	bucket.lastRefill = now

	if bucket.tokens >= 1 {
		bucket.tokens--
		return true
	}
	return false
}

func (gw *Gateway) Produce(ctx context.Context, topic string, partitionKey string, messages []*common.Message) ([]int64, error) {
	start := time.Now()
	defer func() {
		requestTotal.WithLabelValues("produce", topic).Inc()
	}()

	if !gw.rateLimiter.Allow("produce:"+topic, 100000) {
		return nil, ErrRateLimitExceeded
	}

	partition, err := gw.selectPartition(topic, partitionKey)
	if err != nil {
		return nil, err
	}

	leader, ok := gw.router.GetLeader(topic, partition)
	if !ok {
		return nil, ErrNoLeader
	}

	broker, ok := gw.brokerPool.GetBroker(leader)
	if !ok || !broker.healthy {
		return nil, ErrBrokerUnavailable
	}

	return gw.forwardProduce(ctx, broker, topic, partition, messages)
}

func (gw *Gateway) Fetch(ctx context.Context, topic string, partition int32, groupID string, offset int64, maxBytes int32) ([]*common.Message, int64, error) {
	start := time.Now()
	defer func() {
		requestTotal.WithLabelValues("fetch", topic).Inc()
	}()

	if !gw.rateLimiter.Allow("fetch:"+topic, 100000) {
		return nil, -1, ErrRateLimitExceeded
	}

	leader, ok := gw.router.GetLeader(topic, partition)
	if !ok {
		return nil, -1, ErrNoLeader
	}

	broker, ok := gw.brokerPool.GetBroker(leader)
	if !ok || !broker.healthy {
		return nil, -1, ErrBrokerUnavailable
	}

	return gw.forwardFetch(ctx, broker, topic, partition, groupID, offset, maxBytes)
}

func (gw *Gateway) selectPartition(topic string, partitionKey string) (int32, error) {
	return 0, nil
}

func (gw *Gateway) forwardProduce(ctx context.Context, broker *BrokerConn, topic string, partition int32, messages []*common.Message) ([]int64, error) {
	return []int64{0}, nil
}

func (gw *Gateway) forwardFetch(ctx context.Context, broker *BrokerConn, topic string, partition int32, groupID string, offset int64, maxBytes int32) ([]*common.Message, int64, error) {
	return []*common.Message{}, 0, nil
}

func (gw *Gateway) healthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		brokers := gw.brokerPool.ListBrokers()
		for _, b := range brokers {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := b.conn.Invoke(ctx, "health", nil, nil)
			cancel()

			gw.brokerPool.mu.Lock()
			b.healthy = err == nil
			if b.healthy {
				b.lastHB = time.Now()
			}
			gw.brokerPool.mu.Unlock()
		}
	}
}

func (gw *Gateway) Close() {
	gw.mu.Lock()
	gw.running = false
	gw.mu.Unlock()

	brokers := gw.brokerPool.ListBrokers()
	for _, b := range brokers {
		b.conn.Close()
	}
}

func (gw *Gateway) RegisterGRPC(server *grpc.Server) {
}

var (
	ErrRateLimitExceeded  = &GatewayError{Code: 429, Message: "rate limit exceeded"}
	ErrNoLeader           = &GatewayError{Code: 503, Message: "no leader available"}
	ErrBrokerUnavailable  = &GatewayError{Code: 503, Message: "broker unavailable"}
)

type GatewayError struct {
	Code    int
	Message string
}

func (e *GatewayError) Error() string {
	return e.Message
}
