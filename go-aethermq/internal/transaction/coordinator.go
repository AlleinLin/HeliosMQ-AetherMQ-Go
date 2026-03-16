package transaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aethermq/aethermq/internal/common"
	"github.com/aethermq/aethermq/internal/storage"
	"github.com/google/uuid"
)

var (
	ErrTransactionNotFound      = errors.New("transaction not found")
	ErrTransactionAlreadyExists = errors.New("transaction already exists")
	ErrTransactionNotActive     = errors.New("transaction not active")
	ErrTransactionAborted       = errors.New("transaction aborted")
	ErrTransactionCommitted     = errors.New("transaction already committed")
	ErrTransactionTimeout       = errors.New("transaction timeout")
	ErrCoordinatorUnavailable   = errors.New("coordinator unavailable")
)

type TransactionState int32

const (
	TxnStateEmpty TransactionState = iota
	TxnStateOngoing
	TxnStatePrepareCommit
	TxnStatePrepareAbort
	TxnStateCompleteCommit
	TxnStateCompleteAbort
	TxnStateDead
)

func (s TransactionState) String() string {
	switch s {
	case TxnStateEmpty:
		return "Empty"
	case TxnStateOngoing:
		return "Ongoing"
	case TxnStatePrepareCommit:
		return "PrepareCommit"
	case TxnStatePrepareAbort:
		return "PrepareAbort"
	case TxnStateCompleteCommit:
		return "CompleteCommit"
	case TxnStateCompleteAbort:
		return "CompleteAbort"
	case TxnStateDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

type TransactionConfig struct {
	DefaultTimeout       time.Duration
	MaxTimeout           time.Duration
	MaxPartitionsPerTxn  int
	AbortTimedOutTxn     bool
	RemoveExpiredTxn     bool
	TransactionLogDir    string
	CheckpointInterval   time.Duration
}

func DefaultTransactionConfig() *TransactionConfig {
	return &TransactionConfig{
		DefaultTimeout:      15 * time.Minute,
		MaxTimeout:          24 * time.Hour,
		MaxPartitionsPerTxn: 1000,
		AbortTimedOutTxn:    true,
		RemoveExpiredTxn:    true,
		CheckpointInterval:  60 * time.Second,
	}
}

type Transaction struct {
	TxnID           string
	ProducerID      string
	State           TransactionState
	StartTime       int64
	Timeout         time.Duration
	Partitions      map[string]*PartitionTxnRecord
	PendingMessages []*common.Message
	CommitTime      int64
	AbortReason     string
	mu              sync.RWMutex
}

type PartitionTxnRecord struct {
	Topic     string
	Partition int32
	FirstOffset int64
	LastOffset  int64
	Status      TxnPartitionStatus
}

type TxnPartitionStatus int

const (
	TxnPartitionPending TxnPartitionStatus = iota
	TxnPartitionCommitted
	TxnPartitionAborted
)

type TransactionCoordinator struct {
	config        *TransactionConfig
	store         *storage.SegmentStore
	transactions  sync.Map
	stateMachine  *TransactionStateMachine
	timeIndex     *TransactionTimeIndex
	producerTxns  sync.Map
	epoch         atomic.Uint64
	running       atomic.Bool
	logger        interface{}
}

type TransactionStateMachine struct {
	transitions map[TransactionState]map[TransactionState]bool
}

func NewTransactionStateMachine() *TransactionStateMachine {
	return &TransactionStateMachine{
		transitions: map[TransactionState]map[TransactionState]bool{
			TxnStateEmpty: {
				TxnStateOngoing: true,
			},
			TxnStateOngoing: {
				TxnStatePrepareCommit: true,
				TxnStatePrepareAbort:  true,
				TxnStateDead:          true,
			},
			TxnStatePrepareCommit: {
				TxnStateCompleteCommit: true,
				TxnStatePrepareAbort:   true,
			},
			TxnStatePrepareAbort: {
				TxnStateCompleteAbort: true,
			},
			TxnStateCompleteCommit: {},
			TxnStateCompleteAbort:  {},
			TxnStateDead:           {},
		},
	}
}

func (sm *TransactionStateMachine) CanTransition(from, to TransactionState) bool {
	if allowed, ok := sm.transitions[from]; ok {
		return allowed[to]
	}
	return false
}

type TransactionTimeIndex struct {
	mu       sync.RWMutex
	byTime   map[int64][]string
	byTxnID  map[string]int64
}

func NewTransactionTimeIndex() *TransactionTimeIndex {
	return &TransactionTimeIndex{
		byTime:  make(map[int64][]string),
		byTxnID: make(map[string]int64),
	}
}

func (idx *TransactionTimeIndex) Add(txnID string, expireTime int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.byTime[expireTime] = append(idx.byTime[expireTime], txnID)
	idx.byTxnID[txnID] = expireTime
}

func (idx *TransactionTimeIndex) Remove(txnID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if expireTime, ok := idx.byTxnID[txnID]; ok {
		txns := idx.byTime[expireTime]
		for i, id := range txns {
			if id == txnID {
				idx.byTime[expireTime] = append(txns[:i], txns[i+1:]...)
				break
			}
		}
		if len(idx.byTime[expireTime]) == 0 {
			delete(idx.byTime, expireTime)
		}
		delete(idx.byTxnID, txnID)
	}
}

func (idx *TransactionTimeIndex) GetExpired(before int64) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var expired []string
	for t, txns := range idx.byTime {
		if t < before {
			expired = append(expired, txns...)
		}
	}
	return expired
}

func NewTransactionCoordinator(config *TransactionConfig, store *storage.SegmentStore) *TransactionCoordinator {
	tc := &TransactionCoordinator{
		config:       config,
		store:        store,
		stateMachine: NewTransactionStateMachine(),
		timeIndex:    NewTransactionTimeIndex(),
	}
	tc.running.Store(true)
	go tc.backgroundExpirationCheck()
	return tc
}

func (tc *TransactionCoordinator) BeginTransaction(ctx context.Context, producerID string, timeout time.Duration) (string, error) {
	if timeout == 0 {
		timeout = tc.config.DefaultTimeout
	}
	if timeout > tc.config.MaxTimeout {
		timeout = tc.config.MaxTimeout
	}

	txnID := generateTxnID(producerID)

	txn := &Transaction{
		TxnID:           txnID,
		ProducerID:      producerID,
		State:           TxnStateOngoing,
		StartTime:       time.Now().UnixMilli(),
		Timeout:         timeout,
		Partitions:      make(map[string]*PartitionTxnRecord),
		PendingMessages: make([]*common.Message, 0),
	}

	if _, loaded := tc.transactions.LoadOrStore(txnID, txn); loaded {
		return "", ErrTransactionAlreadyExists
	}

	tc.producerTxns.Store(producerID, txnID)

	expireTime := txn.StartTime + timeout.Milliseconds()
	tc.timeIndex.Add(txnID, expireTime)

	tc.epoch.Add(1)

	return txnID, nil
}

func (tc *TransactionCoordinator) AddPartitions(ctx context.Context, txnID string, partitions []struct{ Topic string; Partition int32 }) error {
	txn, err := tc.getTransaction(txnID)
	if err != nil {
		return err
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != TxnStateOngoing {
		return ErrTransactionNotActive
	}

	if len(txn.Partitions)+len(partitions) > tc.config.MaxPartitionsPerTxn {
		return fmt.Errorf("exceeds max partitions per transaction (%d)", tc.config.MaxPartitionsPerTxn)
	}

	for _, p := range partitions {
		key := partitionKey(p.Topic, p.Partition)
		if _, exists := txn.Partitions[key]; !exists {
			txn.Partitions[key] = &PartitionTxnRecord{
				Topic:     p.Topic,
				Partition: p.Partition,
				Status:    TxnPartitionPending,
			}
		}
	}

	return nil
}

func (tc *TransactionCoordinator) AddMessages(ctx context.Context, txnID string, messages []*common.Message) error {
	txn, err := tc.getTransaction(txnID)
	if err != nil {
		return err
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != TxnStateOngoing {
		return ErrTransactionNotActive
	}

	for _, msg := range messages {
		msg.TransactionID = txnID
		key := partitionKey(msg.Topic, msg.Partition)
		if _, exists := txn.Partitions[key]; !exists {
			txn.Partitions[key] = &PartitionTxnRecord{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Status:    TxnPartitionPending,
			}
		}
	}

	txn.PendingMessages = append(txn.PendingMessages, messages...)

	return nil
}

func (tc *TransactionCoordinator) PrepareCommit(ctx context.Context, txnID string) error {
	txn, err := tc.getTransaction(txnID)
	if err != nil {
		return err
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if !tc.stateMachine.CanTransition(txn.State, TxnStatePrepareCommit) {
		return fmt.Errorf("cannot prepare commit from state %s", txn.State)
	}

	txn.State = TxnStatePrepareCommit

	return nil
}

func (tc *TransactionCoordinator) Commit(ctx context.Context, txnID string) error {
	txn, err := tc.getTransaction(txnID)
	if err != nil {
		return err
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if !tc.stateMachine.CanTransition(txn.State, TxnStateCompleteCommit) {
		if txn.State == TxnStateCompleteAbort {
			return ErrTransactionAborted
		}
		return fmt.Errorf("cannot commit from state %s", txn.State)
	}

	for _, msg := range txn.PendingMessages {
		if _, err := tc.store.Append(msg); err != nil {
			txn.State = TxnStatePrepareAbort
			txn.AbortReason = fmt.Sprintf("failed to append message: %v", err)
			return tc.abortInternal(txn)
		}
	}

	for key, pTxn := range txn.Partitions {
		pTxn.Status = TxnPartitionCommitted
		txn.Partitions[key] = pTxn
	}

	txn.State = TxnStateCompleteCommit
	txn.CommitTime = time.Now().UnixMilli()

	tc.timeIndex.Remove(txnID)
	tc.producerTxns.Delete(txn.ProducerID)

	return nil
}

func (tc *TransactionCoordinator) Abort(ctx context.Context, txnID string, reason string) error {
	txn, err := tc.getTransaction(txnID)
	if err != nil {
		return err
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	return tc.abortInternal(txn)
}

func (tc *TransactionCoordinator) abortInternal(txn *Transaction) error {
	if !tc.stateMachine.CanTransition(txn.State, TxnStateCompleteAbort) {
		if txn.State == TxnStateCompleteCommit {
			return ErrTransactionCommitted
		}
		return fmt.Errorf("cannot abort from state %s", txn.State)
	}

	txn.State = TxnStateCompleteAbort
	if txn.AbortReason == "" {
		txn.AbortReason = "user requested"
	}

	for key, pTxn := range txn.Partitions {
		pTxn.Status = TxnPartitionAborted
		txn.Partitions[key] = pTxn
	}

	txn.PendingMessages = nil

	tc.timeIndex.Remove(txn.TxnID)
	tc.producerTxns.Delete(txn.ProducerID)

	return nil
}

func (tc *TransactionCoordinator) GetTransactionState(txnID string) (TransactionState, error) {
	txn, err := tc.getTransaction(txnID)
	if err != nil {
		return TxnStateEmpty, err
	}
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.State, nil
}

func (tc *TransactionCoordinator) GetTransaction(txnID string) (*Transaction, error) {
	return tc.getTransaction(txnID)
}

func (tc *TransactionCoordinator) getTransaction(txnID string) (*Transaction, error) {
	if v, ok := tc.transactions.Load(txnID); ok {
		return v.(*Transaction), nil
	}
	return nil, ErrTransactionNotFound
}

func (tc *TransactionCoordinator) ListTransactions() []*Transaction {
	var txns []*Transaction
	tc.transactions.Range(func(key, value interface{}) bool {
		txns = append(txns, value.(*Transaction))
		return true
	})
	return txns
}

func (tc *TransactionCoordinator) backgroundExpirationCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !tc.running.Load() {
			return
		}
		tc.checkExpiredTransactions()
	}
}

func (tc *TransactionCoordinator) checkExpiredTransactions() {
	if !tc.config.AbortTimedOutTxn {
		return
	}

	now := time.Now().UnixMilli()
	expired := tc.timeIndex.GetExpired(now)

	for _, txnID := range expired {
		txn, err := tc.getTransaction(txnID)
		if err != nil {
			continue
		}

		txn.mu.Lock()
		if txn.State == TxnStateOngoing {
			txn.AbortReason = "transaction timeout"
			tc.abortInternal(txn)
		}
		txn.mu.Unlock()
	}
}

func (tc *TransactionCoordinator) Close() {
	tc.running.Store(false)
}

func generateTxnID(producerID string) string {
	return fmt.Sprintf("txn-%s-%s", producerID, uuid.NewString()[:8])
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

type TransactionLog struct {
	baseDir string
	mu      sync.RWMutex
}

type TxnLogEntry struct {
	TxnID     string
	Op        TxnLogOp
	Data      []byte
	Timestamp int64
}

type TxnLogOp int

const (
	TxnLogBegin TxnLogOp = iota
	TxnLogAddPartition
	TxnLogAddMessage
	TxnLogPrepareCommit
	TxnLogCommit
	TxnLogAbort
	TxnLogMarker
)

func NewTransactionLog(baseDir string) (*TransactionLog, error) {
	return &TransactionLog{baseDir: baseDir}, nil
}

func (l *TransactionLog) Append(entry *TxnLogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return nil
}

func (l *TransactionLog) Read(from, to int64) ([]*TxnLogEntry, error) {
	return []*TxnLogEntry{}, nil
}

func (l *TransactionLog) Truncate(before int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return nil
}

type TransactionMarkerManager struct {
	coordinator *TransactionCoordinator
	store       *storage.SegmentStore
}

func NewTransactionMarkerManager(coordinator *TransactionCoordinator, store *storage.SegmentStore) *TransactionMarkerManager {
	return &TransactionMarkerManager{
		coordinator: coordinator,
		store:       store,
	}
}

func (m *TransactionMarkerManager) WriteCommitMarker(txnID string, topic string, partition int32, offset int64) error {
	marker := &common.Message{
		MessageID: uuid.NewString(),
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Value:     []byte(txnID),
		Headers: map[string]string{
			"__txn_marker":    "commit",
			"__txn_id":        txnID,
			"__marker_offset": fmt.Sprintf("%d", offset),
		},
		Timestamp: time.Now().UnixMilli(),
	}
	_, err := m.store.Append(marker)
	return err
}

func (m *TransactionMarkerManager) WriteAbortMarker(txnID string, topic string, partition int32, offset int64) error {
	marker := &common.Message{
		MessageID: uuid.NewString(),
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Value:     []byte(txnID),
		Headers: map[string]string{
			"__txn_marker":    "abort",
			"__txn_id":        txnID,
			"__marker_offset": fmt.Sprintf("%d", offset),
		},
		Timestamp: time.Now().UnixMilli(),
	}
	_, err := m.store.Append(marker)
	return err
}

func (m *TransactionMarkerManager) IsTransactionalMessage(msg *common.Message) bool {
	return msg.TransactionID != ""
}

func (m *TransactionMarkerManager) IsTransactionMarker(msg *common.Message) bool {
	_, ok := msg.Headers["__txn_marker"]
	return ok
}

func (m *TransactionMarkerManager) ParseMarker(msg *common.Message) (txnID string, isCommit bool, err error) {
	markerType, ok := msg.Headers["__txn_marker"]
	if !ok {
		return "", false, errors.New("not a transaction marker")
	}
	txnID = msg.Headers["__txn_id"]
	isCommit = markerType == "commit"
	return txnID, isCommit, nil
}
