package tracing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aethermq/aethermq/internal/common"
	"github.com/google/uuid"
)

var (
	ErrTraceNotFound      = errors.New("trace not found")
	ErrTraceExpired       = errors.New("trace expired")
	ErrInvalidTraceFormat = errors.New("invalid trace format")
)

type TraceEventType string

const (
	TraceEventProduce       TraceEventType = "PRODUCE"
	TraceEventReplicate     TraceEventType = "REPLICATE"
	TraceEventConsume       TraceEventType = "CONSUME"
	TraceEventAck           TraceEventType = "ACK"
	TraceEventRetry         TraceEventType = "RETRY"
	TraceEventDLQ           TraceEventType = "DLQ"
	TraceEventExpire        TraceEventType = "EXPIRE"
	TraceEventDelaySchedule TraceEventType = "DELAY_SCHEDULE"
	TraceEventDelayDeliver  TraceEventType = "DELAY_DELIVER"
	TraceEventTxnBegin      TraceEventType = "TXN_BEGIN"
	TraceEventTxnCommit     TraceEventType = "TXN_COMMIT"
	TraceEventTxnAbort      TraceEventType = "TXN_ABORT"
)

type TraceEvent struct {
	EventID     string                 `json:"event_id"`
	MessageID   string                 `json:"message_id"`
	TraceID     string                 `json:"trace_id"`
	EventType   TraceEventType         `json:"event_type"`
	Topic       string                 `json:"topic"`
	Partition   int32                  `json:"partition"`
	Offset      int64                  `json:"offset"`
	Timestamp   int64                  `json:"timestamp"`
	BrokerID    string                 `json:"broker_id"`
	ClientID    string                 `json:"client_id"`
	ProducerID  string                 `json:"producer_id"`
	ConsumerID  string                 `json:"consumer_id"`
	GroupID     string                 `json:"group_id"`
	Properties  map[string]string      `json:"properties"`
	Metrics     *TraceMetrics          `json:"metrics,omitempty"`
	ParentEvent string                 `json:"parent_event,omitempty"`
	Children    []string               `json:"children,omitempty"`
}

type TraceMetrics struct {
	LatencyMs       int64  `json:"latency_ms"`
	MessageSize     int64  `json:"message_size"`
	ProcessingTime  int64  `json:"processing_time"`
	QueueTime       int64  `json:"queue_time"`
	RetryCount      int32  `json:"retry_count"`
	ReplicationLag  int64  `json:"replication_lag"`
}

type TraceConfig struct {
	Enabled           bool
	SampleRate        float64
	RetentionDays     int
	MaxEventsPerTrace int
	StorageBackend    string
	IndexFields       []string
	BatchSize         int
	FlushInterval     time.Duration
}

func DefaultTraceConfig() *TraceConfig {
	return &TraceConfig{
		Enabled:           true,
		SampleRate:        1.0,
		RetentionDays:     7,
		MaxEventsPerTrace: 1000,
		StorageBackend:    "rocksdb",
		IndexFields: []string{
			"message_id", "trace_id", "topic", "partition",
			"producer_id", "consumer_id", "group_id",
		},
		BatchSize:     1000,
		FlushInterval: 100 * time.Millisecond,
	}
}

type TraceContext struct {
	TraceID  string
	SpanID   string
	ParentID string
	Sampled  bool
}

func NewTraceContext() *TraceContext {
	return &TraceContext{
		TraceID: uuid.NewString(),
		SpanID:  generateSpanID(),
		Sampled: true,
	}
}

func (ctx *TraceContext) ChildSpan() *TraceContext {
	return &TraceContext{
		TraceID:  ctx.TraceID,
		SpanID:   generateSpanID(),
		ParentID: ctx.SpanID,
		Sampled:  ctx.Sampled,
	}
}

func (ctx *TraceContext) ToHeaders() map[string]string {
	return map[string]string{
		"trace-id":  ctx.TraceID,
		"span-id":   ctx.SpanID,
		"parent-id": ctx.ParentID,
		"sampled":   fmt.Sprintf("%v", ctx.Sampled),
	}
}

func TraceContextFromHeaders(headers map[string]string) *TraceContext {
	ctx := &TraceContext{}
	if v, ok := headers["trace-id"]; ok {
		ctx.TraceID = v
	}
	if v, ok := headers["span-id"]; ok {
		ctx.SpanID = v
	}
	if v, ok := headers["parent-id"]; ok {
		ctx.ParentID = v
	}
	if v, ok := headers["sampled"]; ok {
		ctx.Sampled = v == "true"
	}
	return ctx
}

type MessageTracer struct {
	config      *TraceConfig
	store       TraceStore
	indexer     *TraceIndexer
	sampler     *TraceSampler
	eventBuffer *EventBuffer
	running     bool
	mu          sync.RWMutex
}

func NewMessageTracer(config *TraceConfig) (*MessageTracer, error) {
	store, err := NewRocksDBTraceStore(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace store: %w", err)
	}

	mt := &MessageTracer{
		config:      config,
		store:       store,
		indexer:     NewTraceIndexer(config.IndexFields),
		sampler:     NewTraceSampler(config.SampleRate),
		eventBuffer: NewEventBuffer(config.BatchSize, config.FlushInterval),
		running:     true,
	}

	go mt.backgroundFlush()
	go mt.backgroundCleanup()

	return mt, nil
}

func (mt *MessageTracer) TraceProduce(ctx context.Context, msg *common.Message, brokerID, clientID string) error {
	if !mt.config.Enabled {
		return nil
	}

	if !mt.sampler.ShouldSample() {
		return nil
	}

	traceCtx := NewTraceContext()
	if msg.Headers != nil {
		for k, v := range traceCtx.ToHeaders() {
			msg.Headers[k] = v
		}
	}
	msg.TraceID = traceCtx.TraceID

	event := &TraceEvent{
		EventID:    uuid.NewString(),
		MessageID:  msg.MessageID,
		TraceID:    traceCtx.TraceID,
		EventType:  TraceEventProduce,
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		Timestamp:  time.Now().UnixMilli(),
		BrokerID:   brokerID,
		ClientID:   clientID,
		ProducerID: msg.ProducerID,
		Properties: copyHeaders(msg.Headers),
		Metrics: &TraceMetrics{
			MessageSize: int64(len(msg.Value)),
		},
	}

	return mt.recordEvent(event)
}

func (mt *MessageTracer) TraceReplicate(ctx context.Context, msg *common.Message, brokerID string, sourceBroker string) error {
	if !mt.config.Enabled || msg.TraceID == "" {
		return nil
	}

	event := &TraceEvent{
		EventID:   uuid.NewString(),
		MessageID: msg.MessageID,
		TraceID:   msg.TraceID,
		EventType: TraceEventReplicate,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: time.Now().UnixMilli(),
		BrokerID:  brokerID,
		Properties: map[string]string{
			"source_broker": sourceBroker,
		},
	}

	return mt.recordEvent(event)
}

func (mt *MessageTracer) TraceConsume(ctx context.Context, msg *common.Message, brokerID, clientID, groupID string) error {
	if !mt.config.Enabled || msg.TraceID == "" {
		return nil
	}

	event := &TraceEvent{
		EventID:   uuid.NewString(),
		MessageID: msg.MessageID,
		TraceID:   msg.TraceID,
		EventType: TraceEventConsume,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: time.Now().UnixMilli(),
		BrokerID:  brokerID,
		ClientID:  clientID,
		GroupID:   groupID,
		Properties: copyHeaders(msg.Headers),
	}

	return mt.recordEvent(event)
}

func (mt *MessageTracer) TraceAck(ctx context.Context, msg *common.Message, clientID, groupID string) error {
	if !mt.config.Enabled || msg.TraceID == "" {
		return nil
	}

	event := &TraceEvent{
		EventID:   uuid.NewString(),
		MessageID: msg.MessageID,
		TraceID:   msg.TraceID,
		EventType: TraceEventAck,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: time.Now().UnixMilli(),
		ClientID:  clientID,
		GroupID:   groupID,
	}

	return mt.recordEvent(event)
}

func (mt *MessageTracer) TraceRetry(ctx context.Context, msg *common.Message, reason string) error {
	if !mt.config.Enabled || msg.TraceID == "" {
		return nil
	}

	event := &TraceEvent{
		EventID:   uuid.NewString(),
		MessageID: msg.MessageID,
		TraceID:   msg.TraceID,
		EventType: TraceEventRetry,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: time.Now().UnixMilli(),
		Properties: map[string]string{
			"reason":      reason,
			"retry_count": fmt.Sprintf("%d", msg.RetryCount),
		},
		Metrics: &TraceMetrics{
			RetryCount: msg.RetryCount,
		},
	}

	return mt.recordEvent(event)
}

func (mt *MessageTracer) TraceDLQ(ctx context.Context, msg *common.Message, groupID, reason string) error {
	if !mt.config.Enabled || msg.TraceID == "" {
		return nil
	}

	event := &TraceEvent{
		EventID:   uuid.NewString(),
		MessageID: msg.MessageID,
		TraceID:   msg.TraceID,
		EventType: TraceEventDLQ,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: time.Now().UnixMilli(),
		GroupID:   groupID,
		Properties: map[string]string{
			"reason": reason,
		},
	}

	return mt.recordEvent(event)
}

func (mt *MessageTracer) TraceDelay(ctx context.Context, msg *common.Message, scheduleTime, deliverTime int64) error {
	if !mt.config.Enabled || msg.TraceID == "" {
		return nil
	}

	scheduleEvent := &TraceEvent{
		EventID:   uuid.NewString(),
		MessageID: msg.MessageID,
		TraceID:   msg.TraceID,
		EventType: TraceEventDelaySchedule,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Timestamp: scheduleTime,
		Properties: map[string]string{
			"deliver_at": fmt.Sprintf("%d", deliverTime),
		},
	}
	mt.recordEvent(scheduleEvent)

	deliverEvent := &TraceEvent{
		EventID:   uuid.NewString(),
		MessageID: msg.MessageID,
		TraceID:   msg.TraceID,
		EventType: TraceEventDelayDeliver,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Timestamp: deliverTime,
		Metrics: &TraceMetrics{
			QueueTime: deliverTime - scheduleTime,
		},
	}

	return mt.recordEvent(deliverEvent)
}

func (mt *MessageTracer) TraceTransaction(ctx context.Context, txnID string, eventType TraceEventType, partitions []struct{ Topic string; Partition int32 }) error {
	if !mt.config.Enabled {
		return nil
	}

	event := &TraceEvent{
		EventID:   uuid.NewString(),
		TraceID:   txnID,
		EventType: eventType,
		Timestamp: time.Now().UnixMilli(),
		Properties: map[string]string{
			"partition_count": fmt.Sprintf("%d", len(partitions)),
		},
	}

	return mt.recordEvent(event)
}

func (mt *MessageTracer) GetTrace(traceID string) (*Trace, error) {
	events, err := mt.store.GetByTraceID(traceID)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, ErrTraceNotFound
	}

	trace := &Trace{
		TraceID: traceID,
		Events:  events,
	}

	trace.buildTree()

	return trace, nil
}

func (mt *MessageTracer) GetTraceByMessage(messageID string) (*Trace, error) {
	events, err := mt.store.GetByMessageID(messageID)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, ErrTraceNotFound
	}

	trace := &Trace{
		TraceID: events[0].TraceID,
		Events:  events,
	}

	trace.buildTree()

	return trace, nil
}

func (mt *MessageTracer) QueryTraces(query *TraceQuery) (*TraceQueryResult, error) {
	return mt.indexer.Query(query)
}

func (mt *MessageTracer) recordEvent(event *TraceEvent) error {
	if err := mt.eventBuffer.Add(event); err != nil {
		return err
	}

	mt.indexer.Index(event)

	return nil
}

func (mt *MessageTracer) backgroundFlush() {
	for mt.running {
		events := mt.eventBuffer.Flush()
		if len(events) > 0 {
			mt.store.BatchWrite(events)
		}
		time.Sleep(mt.config.FlushInterval)
	}
}

func (mt *MessageTracer) backgroundCleanup() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		if !mt.running {
			return
		}
		cutoff := time.Now().AddDate(0, 0, -mt.config.RetentionDays)
		mt.store.DeleteBefore(cutoff.UnixMilli())
	}
}

func (mt *MessageTracer) Close() error {
	mt.running = false
	return mt.store.Close()
}

type Trace struct {
	TraceID   string
	Events    []*TraceEvent
	RootEvent *TraceEvent
	Duration  int64
}

func (t *Trace) buildTree() {
	eventMap := make(map[string]*TraceEvent)
	for _, event := range t.Events {
		eventMap[event.EventID] = event
	}

	for _, event := range t.Events {
		if event.ParentEvent != "" {
			if parent, ok := eventMap[event.ParentEvent]; ok {
				parent.Children = append(parent.Children, event.EventID)
			}
		} else {
			t.RootEvent = event
		}
	}

	if len(t.Events) > 0 {
		t.Duration = t.Events[len(t.Events)-1].Timestamp - t.Events[0].Timestamp
	}
}

func (t *Trace) ToJSON() (string, error) {
	data, err := json.MarshalIndent(t, "", "  ")
	return string(data), err
}

func (t *Trace) GetEventByType(eventType TraceEventType) []*TraceEvent {
	var events []*TraceEvent
	for _, e := range t.Events {
		if e.EventType == eventType {
			events = append(events, e)
		}
	}
	return events
}

func (t *Trace) CalculateLatency() map[TraceEventType]int64 {
	latencies := make(map[TraceEventType]int64)
	var produceTime int64

	for _, e := range t.Events {
		if e.EventType == TraceEventProduce {
			produceTime = e.Timestamp
		}
		if produceTime > 0 {
			latencies[e.EventType] = e.Timestamp - produceTime
		}
	}

	return latencies
}

type TraceQuery struct {
	TraceID      string
	MessageID    string
	Topic        string
	Partition    *int32
	ProducerID   string
	ConsumerID   string
	GroupID      string
	EventType    TraceEventType
	StartTime    int64
	EndTime      int64
	Limit        int
	Offset       int
}

type TraceQueryResult struct {
	Traces    []*Trace
	Total     int
	Limit     int
	Offset    int
}

type TraceStore interface {
	Write(event *TraceEvent) error
	BatchWrite(events []*TraceEvent) error
	GetByTraceID(traceID string) ([]*TraceEvent, error)
	GetByMessageID(messageID string) ([]*TraceEvent, error)
	DeleteBefore(timestamp int64) error
	Close() error
}

type RocksDBTraceStore struct {
	baseDir string
	mu      sync.RWMutex
}

func NewRocksDBTraceStore(config *TraceConfig) (*RocksDBTraceStore, error) {
	return &RocksDBTraceStore{
		baseDir: "./traces",
	}, nil
}

func (s *RocksDBTraceStore) Write(event *TraceEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

func (s *RocksDBTraceStore) BatchWrite(events []*TraceEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

func (s *RocksDBTraceStore) GetByTraceID(traceID string) ([]*TraceEvent, error) {
	return []*TraceEvent{}, nil
}

func (s *RocksDBTraceStore) GetByMessageID(messageID string) ([]*TraceEvent, error) {
	return []*TraceEvent{}, nil
}

func (s *RocksDBTraceStore) DeleteBefore(timestamp int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

func (s *RocksDBTraceStore) Close() error {
	return nil
}

type TraceIndexer struct {
	fields  []string
	indices map[string]map[string][]string
	mu      sync.RWMutex
}

func NewTraceIndexer(fields []string) *TraceIndexer {
	indices := make(map[string]map[string][]string)
	for _, field := range fields {
		indices[field] = make(map[string][]string)
	}
	return &TraceIndexer{
		fields:  fields,
		indices: indices,
	}
}

func (idx *TraceIndexer) Index(event *TraceEvent) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.indices["trace_id"] != nil {
		idx.indices["trace_id"][event.TraceID] = append(idx.indices["trace_id"][event.TraceID], event.EventID)
	}
	if idx.indices["message_id"] != nil {
		idx.indices["message_id"][event.MessageID] = append(idx.indices["message_id"][event.MessageID], event.EventID)
	}
	if idx.indices["topic"] != nil {
		key := fmt.Sprintf("%s:%d", event.Topic, event.Partition)
		idx.indices["topic"][key] = append(idx.indices["topic"][key], event.EventID)
	}
}

func (idx *TraceIndexer) Query(query *TraceQuery) (*TraceQueryResult, error) {
	return &TraceQueryResult{}, nil
}

type TraceSampler struct {
	rate     float64
	counter  uint64
}

func NewTraceSampler(rate float64) *TraceSampler {
	if rate > 1.0 {
		rate = 1.0
	}
	if rate < 0 {
		rate = 0
	}
	return &TraceSampler{rate: rate}
}

func (s *TraceSampler) ShouldSample() bool {
	if s.rate >= 1.0 {
		return true
	}
	if s.rate <= 0 {
		return false
	}
	s.counter++
	return float64(s.counter%100)/100.0 < s.rate
}

type EventBuffer struct {
	events      []*TraceEvent
	batchSize   int
	flushInterval time.Duration
	mu          sync.Mutex
}

func NewEventBuffer(batchSize int, flushInterval time.Duration) *EventBuffer {
	return &EventBuffer{
		events:      make([]*TraceEvent, 0, batchSize),
		batchSize:   batchSize,
		flushInterval: flushInterval,
	}
}

func (b *EventBuffer) Add(event *TraceEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, event)
	return nil
}

func (b *EventBuffer) Flush() []*TraceEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.events) == 0 {
		return nil
	}

	events := b.events
	b.events = make([]*TraceEvent, 0, b.batchSize)
	return events
}

func generateSpanID() string {
	return uuid.NewString()[:16]
}

func copyHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return nil
	}
	copy := make(map[string]string, len(headers))
	for k, v := range headers {
		copy[k] = v
	}
	return copy
}
