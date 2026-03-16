package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aethermq/aethermq/internal/broker"
	"github.com/aethermq/aethermq/internal/common"
	"github.com/aethermq/aethermq/internal/metadata"
)

var (
	ErrInvalidRequest       = errors.New("invalid request")
	ErrUnsupportedVersion   = errors.New("unsupported api version")
	ErrUnknownAPIKey        = errors.New("unknown api key")
	ErrTopicNotFound        = errors.New("topic not found")
	ErrPartitionNotFound    = errors.New("partition not found")
	ErrNotLeaderForPartition = errors.New("not leader for partition")
)

const (
	APIKeyProduce          int16 = 0
	APIKeyFetch            int16 = 1
	APIKeyListOffsets      int16 = 2
	APIKeyMetadata         int16 = 3
	APIKeyLeaderAndISR     int16 = 4
	APIKeyStopReplica      int16 = 5
	APIKeyUpdateMetadata   int16 = 6
	APIKeyControlledShutdown int16 = 7
	APIKeyOffsetCommit     int16 = 8
	APIKeyOffsetFetch      int16 = 9
	APIKeyFindCoordinator  int16 = 10
	APIKeyJoinGroup        int16 = 11
	APIKeyHeartbeat        int16 = 12
	APIKeyLeaveGroup       int16 = 13
	APIKeySyncGroup        int16 = 14
	APIKeyDescribeGroups   int16 = 15
	APIKeyListGroups       int16 = 16
	APIKeySaslHandshake    int16 = 17
	APIKeyApiVersions      int16 = 18
	APIKeyCreateTopics     int16 = 19
	APIKeyDeleteTopics     int16 = 20
	APIKeyDeleteRecords    int16 = 21
	APIKeyInitProducerID   int16 = 22
	APIKeyOffsetForLeaderEpoch int16 = 23
	APIKeyAddPartitionsToTxn int16 = 24
	APIKeyAddOffsetsToTxn  int16 = 25
	APIKeyEndTxn           int16 = 26
	APIKeyWriteTxnMarkers  int16 = 27
	APIKeyTxnOffsetCommit  int16 = 28
	APIKeyDescribeAcls     int16 = 29
	APIKeyCreateAcls       int16 = 30
	APIKeyDeleteAcls       int16 = 31
	APIKeyDescribeConfigs  int16 = 32
	APIKeyAlterConfigs     int16 = 33
	APIKeyAlterReplicaLogDirs int16 = 34
	APIKeyDescribeLogDirs  int16 = 35
	APIKeySaslAuthenticate int16 = 36
	APIKeyCreatePartitions int16 = 37
	APIKeyCreateDelegationToken int16 = 38
	APIKeyRenewDelegationToken int16 = 39
	APIKeyExpireDelegationToken int16 = 40
	APIKeyDescribeDelegationToken int16 = 41
	APIKeyDeleteGroups     int16 = 42
)

type KafkaProtocolConfig struct {
	ListenAddr         string
	MaxConnections     int
	MaxRequestSize     int32
	MaxResponseSize    int32
	RequestTimeout     time.Duration
	SocketSendBuffer   int
	SocketRecvBuffer   int
	EnableSASL         bool
	EnableTLS          bool
}

func DefaultKafkaProtocolConfig() *KafkaProtocolConfig {
	return &KafkaProtocolConfig{
		ListenAddr:      ":9092",
		MaxConnections:  10000,
		MaxRequestSize:  100 * 1024 * 1024,
		MaxResponseSize: 100 * 1024 * 1024,
		RequestTimeout:  30 * time.Second,
		SocketSendBuffer: 256 * 1024,
		SocketRecvBuffer: 256 * 1024,
	}
}

type KafkaProtocolServer struct {
	config     *KafkaProtocolConfig
	broker     *broker.Broker
	metadata   *metadata.BoltStore
	listener   net.Listener
	clients    sync.Map
	connCount  atomic.Int32
	running    atomic.Bool
	apiVersions map[int16]int16
}

func NewKafkaProtocolServer(config *KafkaProtocolConfig, b *broker.Broker, meta *metadata.BoltStore) *KafkaProtocolServer {
	return &KafkaProtocolServer{
		config:      config,
		broker:      b,
		metadata:    meta,
		apiVersions: defaultAPIVersions(),
	}
}

func defaultAPIVersions() map[int16]int16 {
	return map[int16]int16{
		APIKeyProduce:       9,
		APIKeyFetch:         12,
		APIKeyListOffsets:   6,
		APIKeyMetadata:      11,
		APIKeyOffsetCommit:  8,
		APIKeyOffsetFetch:   8,
		APIKeyFindCoordinator: 4,
		APIKeyJoinGroup:     7,
		APIKeyHeartbeat:     4,
		APIKeyLeaveGroup:    4,
		APIKeySyncGroup:     5,
		APIKeyDescribeGroups: 5,
		APIKeyListGroups:    4,
		APIKeyApiVersions:   3,
		APIKeyCreateTopics:  7,
		APIKeyDeleteTopics:  6,
		APIKeyInitProducerID: 5,
		APIKeyEndTxn:        3,
	}
}

func (s *KafkaProtocolServer) Start() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener
	s.running.Store(true)

	go s.acceptLoop()
	return nil
}

func (s *KafkaProtocolServer) acceptLoop() {
	for s.running.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running.Load() {
				continue
			}
			return
		}

		if int(s.connCount.Load()) >= s.config.MaxConnections {
			conn.Close()
			continue
		}

		s.connCount.Add(1)
		clientID := conn.RemoteAddr().String()
		s.clients.Store(clientID, conn)

		go s.handleConnection(conn, clientID)
	}
}

func (s *KafkaProtocolServer) handleConnection(conn net.Conn, clientID string) {
	defer func() {
		conn.Close()
		s.clients.Delete(clientID)
		s.connCount.Add(-1)
	}()

	buf := make([]byte, 4)

	for s.running.Load() {
		if err := conn.SetReadDeadline(time.Now().Add(s.config.RequestTimeout)); err != nil {
			return
		}

		_, err := io.ReadFull(conn, buf)
		if err != nil {
			return
		}

		size := int32(binary.BigEndian.Uint32(buf))
		if size > s.config.MaxRequestSize || size < 0 {
			return
		}

		reqBuf := make([]byte, size)
		if _, err := io.ReadFull(conn, reqBuf); err != nil {
			return
		}

		response := s.processRequest(reqBuf)

		if err := conn.SetWriteDeadline(time.Now().Add(s.config.RequestTimeout)); err != nil {
			return
		}

		respSize := make([]byte, 4)
		binary.BigEndian.PutUint32(respSize, uint32(len(response)))
		if _, err := conn.Write(respSize); err != nil {
			return
		}
		if _, err := conn.Write(response); err != nil {
			return
		}
	}
}

func (s *KafkaProtocolServer) processRequest(data []byte) []byte {
	reader := NewRequestReader(data)

	apiKey, err := reader.ReadInt16()
	if err != nil {
		return s.errorResponse(0, 0, ErrInvalidRequest)
	}

	apiVersion, err := reader.ReadInt16()
	if err != nil {
		return s.errorResponse(0, 0, ErrInvalidRequest)
	}

	correlationID, err := reader.ReadInt32()
	if err != nil {
		return s.errorResponse(0, 0, ErrInvalidRequest)
	}

	clientID, _ := reader.ReadString()

	maxVersion, ok := s.apiVersions[apiKey]
	if !ok {
		return s.errorResponse(correlationID, apiVersion, ErrUnknownAPIKey)
	}

	if apiVersion > maxVersion {
		return s.errorResponse(correlationID, apiVersion, ErrUnsupportedVersion)
	}

	var response []byte

	switch apiKey {
	case APIKeyApiVersions:
		response = s.handleApiVersions(correlationID, apiVersion)
	case APIKeyMetadata:
		response = s.handleMetadata(correlationID, apiVersion, reader)
	case APIKeyProduce:
		response = s.handleProduce(correlationID, apiVersion, reader, clientID)
	case APIKeyFetch:
		response = s.handleFetch(correlationID, apiVersion, reader, clientID)
	case APIKeyListOffsets:
		response = s.handleListOffsets(correlationID, apiVersion, reader)
	case APIKeyOffsetCommit:
		response = s.handleOffsetCommit(correlationID, apiVersion, reader)
	case APIKeyOffsetFetch:
		response = s.handleOffsetFetch(correlationID, apiVersion, reader)
	case APIKeyFindCoordinator:
		response = s.handleFindCoordinator(correlationID, apiVersion, reader)
	case APIKeyJoinGroup:
		response = s.handleJoinGroup(correlationID, apiVersion, reader, clientID)
	case APIKeyHeartbeat:
		response = s.handleHeartbeat(correlationID, apiVersion, reader)
	case APIKeyLeaveGroup:
		response = s.handleLeaveGroup(correlationID, apiVersion, reader)
	case APIKeySyncGroup:
		response = s.handleSyncGroup(correlationID, apiVersion, reader)
	case APIKeyCreateTopics:
		response = s.handleCreateTopics(correlationID, apiVersion, reader)
	case APIKeyDeleteTopics:
		response = s.handleDeleteTopics(correlationID, apiVersion, reader)
	case APIKeyInitProducerID:
		response = s.handleInitProducerID(correlationID, apiVersion, reader)
	case APIKeyEndTxn:
		response = s.handleEndTxn(correlationID, apiVersion, reader)
	default:
		response = s.errorResponse(correlationID, apiVersion, ErrUnknownAPIKey)
	}

	return response
}

func (s *KafkaProtocolServer) handleApiVersions(correlationID int32, apiVersion int16) []byte {
	writer := NewResponseWriter()

	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	count := int32(len(s.apiVersions))
	writer.WriteInt32(count)

	for key, version := range s.apiVersions {
		writer.WriteInt16(key)
		writer.WriteInt16(0)
		writer.WriteInt16(version)
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleMetadata(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	var topics []string
	if apiVersion >= 1 {
		topicCount, _ := reader.ReadInt32()
		for i := int32(0); i < topicCount; i++ {
			topic, _ := reader.ReadString()
			topics = append(topics, topic)
		}
	}

	if len(topics) == 0 {
		topicsList, _ := s.metadata.ListTopics()
		for _, t := range topicsList {
			topics = append(topics, t.Name)
		}
	}

	brokers, _ := s.metadata.ListBrokers()

	writer.WriteInt32(int32(len(brokers)))
	for _, b := range brokers {
		writer.WriteInt32(0)
		writer.WriteString(b.Address)
		writer.WriteInt32(9092)
		if apiVersion >= 1 {
			writer.WriteString("")
		}
	}

	if apiVersion >= 2 {
		writer.WriteInt32(-1)
	}

	writer.WriteInt32(int32(len(topics)))
	for _, topicName := range topics {
		writer.WriteInt16(0)
		writer.WriteString(topicName)

		topicMeta, err := s.metadata.GetTopic(topicName)
		if err != nil {
			writer.WriteInt16(3)
			writer.WriteInt32(0)
			continue
		}

		writer.WriteInt16(0)
		writer.WriteInt32(topicMeta.Partitions)

		partitionCount := topicMeta.Partitions
		writer.WriteInt32(partitionCount)

		for p := int32(0); p < partitionCount; p++ {
			writer.WriteInt16(0)
			writer.WriteInt32(p)
			writer.WriteInt32(0)
			writer.WriteInt32(1)
			writer.WriteInt32(0)
			writer.WriteInt32(1)
			writer.WriteInt32(0)
			if apiVersion >= 5 {
				writer.WriteInt32(-1)
			}
		}
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleProduce(correlationID int32, apiVersion int16, reader *RequestReader, clientID string) []byte {
	acks, _ := reader.ReadInt16()
	timeout, _ := reader.ReadInt32()

	if apiVersion >= 1 {
		_, _ = reader.ReadInt32()
	}

	topicCount, _ := reader.ReadInt32()

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	if apiVersion >= 1 {
		writer.WriteInt32(0)
	}

	writer.WriteInt32(topicCount)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := reader.ReadString()
		partitionCount, _ := reader.ReadInt32()

		writer.WriteString(topic)
		writer.WriteInt32(partitionCount)

		for j := int32(0); j < partitionCount; j++ {
			partition, _ := reader.ReadInt32()
			recordSetSize, _ := reader.ReadInt32()

			records := make([][]byte, 0)
			if recordSetSize > 0 {
				recordData := make([]byte, recordSetSize)
				reader.Read(recordData)
				records = parseRecordBatch(recordData)
			}

			var baseOffset int64 = 0
			var errorCode int16 = 0

			if len(records) > 0 {
				messages := make([]*common.Message, len(records))
				for k, record := range records {
					messages[k] = &common.Message{
						Topic:     topic,
						Partition: partition,
						Value:     record,
						Headers:   make(map[string]string),
					}
				}

				offsets, err := s.broker.Produce(context.Background(), topic, "", messages)
				if err != nil {
					errorCode = 1
				} else if len(offsets) > 0 {
					baseOffset = offsets[0]
				}
			}

			writer.WriteInt16(errorCode)
			writer.WriteInt64(baseOffset)
			if apiVersion >= 2 {
				writer.WriteInt64(-1)
			}
			if apiVersion >= 5 {
				writer.WriteInt64(-1)
			}
		}
	}

	if apiVersion >= 1 {
		writer.WriteInt32(0)
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleFetch(correlationID int32, apiVersion int16, reader *RequestReader, clientID string) []byte {
	maxWait, _ := reader.ReadInt32()
	minBytes, _ := reader.ReadInt32()
	if apiVersion >= 3 {
		maxBytes, _ := reader.ReadInt32()
		_ = maxBytes
	}
	if apiVersion >= 4 {
		isolation, _ := reader.ReadInt8()
		_ = isolation
	}
	if apiVersion >= 7 {
		_, _ = reader.ReadInt32()
		_, _ = reader.ReadInt64()
	}

	topicCount, _ := reader.ReadInt32()

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	if apiVersion >= 1 {
		writer.WriteInt32(0)
	}

	writer.WriteInt32(topicCount)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := reader.ReadString()
		partitionCount, _ := reader.ReadInt32()

		writer.WriteString(topic)
		writer.WriteInt32(partitionCount)

		for j := int32(0); j < partitionCount; j++ {
			partition, _ := reader.ReadInt32()
			fetchOffset, _ := reader.ReadInt64()
			partitionMaxBytes, _ := reader.ReadInt32()
			if apiVersion >= 5 {
				logStartOffset, _ := reader.ReadInt64()
				_ = logStartOffset
			}

			messages, leo, err := s.broker.Fetch(context.Background(), topic, partition, "", fetchOffset, partitionMaxBytes)

			writer.WriteInt16(0)
			writer.WriteInt64(fetchOffset)
			if apiVersion >= 4 {
				writer.WriteInt64(0)
			}
			if apiVersion >= 5 {
				writer.WriteInt64(0)
			}

			if err != nil || len(messages) == 0 {
				writer.WriteInt32(0)
				continue
			}

			recordBatch := buildRecordBatch(messages, fetchOffset)
			writer.WriteInt32(int32(len(recordBatch)))
			writer.Write(recordBatch)

			_ = maxWait
			_ = minBytes
			_ = leo
		}
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleListOffsets(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	_ = reader

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)
	writer.WriteInt32(0)

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleOffsetCommit(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	groupID, _ := reader.ReadString()

	if apiVersion >= 1 {
		_, _ = reader.ReadInt32()
		_, _ = reader.ReadString()
		_, _ = reader.ReadInt64()
	}

	topicCount, _ := reader.ReadInt32()

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	writer.WriteInt32(topicCount)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := reader.ReadString()
		partitionCount, _ := reader.ReadInt32()

		writer.WriteString(topic)
		writer.WriteInt32(partitionCount)

		for j := int32(0); j < partitionCount; j++ {
			partition, _ := reader.ReadInt32()
			offset, _ := reader.ReadInt64()
			_, _ = reader.ReadString()
			if apiVersion >= 2 {
				_, _ = reader.ReadString()
			}

			s.broker.CommitOffset(context.Background(), groupID, topic, partition, offset)

			writer.WriteInt16(0)
		}
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleOffsetFetch(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	groupID, _ := reader.ReadString()

	var topics []string
	if apiVersion >= 1 {
		_, _ = reader.ReadString()
	}

	topicCount, _ := reader.ReadInt32()
	for i := int32(0); i < topicCount; i++ {
		topic, _ := reader.ReadString()
		topics = append(topics, topic)
		partitionCount, _ := reader.ReadInt32()
		for j := int32(0); j < partitionCount; j++ {
			_, _ = reader.ReadInt32()
		}
	}

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	writer.WriteInt32(int32(len(topics)))
	for _, topic := range topics {
		writer.WriteString(topic)
		writer.WriteInt32(1)
		writer.WriteInt32(0)
		offset, _ := s.broker.GetOffset(context.Background(), groupID, topic, 0)
		writer.WriteInt64(offset)
		writer.WriteString("")
		writer.WriteInt16(0)
	}

	if apiVersion >= 3 {
		writer.WriteInt16(0)
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleFindCoordinator(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	_, _ = reader.ReadString()
	_, _ = reader.ReadInt8()

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)
	writer.WriteInt16(0)
	writer.WriteInt32(0)
	writer.WriteString("localhost")
	writer.WriteInt32(9092)

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleJoinGroup(correlationID int32, apiVersion int16, reader *RequestReader, clientID string) []byte {
	groupID, _ := reader.ReadString()
	sessionTimeout, _ := reader.ReadInt32()
	if apiVersion >= 1 {
		rebalanceTimeout, _ := reader.ReadInt32()
		_ = rebalanceTimeout
	}
	_, _ = reader.ReadString()
	_, _ = reader.ReadString()
	_, _ = reader.ReadString()
	_, _ = reader.ReadInt32()

	_ = sessionTimeout

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)
	writer.WriteInt16(0)
	writer.WriteInt32(1)
	writer.WriteString("member-1")
	writer.WriteString("member-1")
	writer.WriteInt32(0)

	s.broker.JoinGroup(context.Background(), groupID, "member-1", clientID, []string{})

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleHeartbeat(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	groupID, _ := reader.ReadString()
	_, _ = reader.ReadInt32()
	memberID, _ := reader.ReadString()
	_, _ = reader.ReadString()

	s.broker.Heartbeat(context.Background(), groupID, memberID)

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleLeaveGroup(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	groupID, _ := reader.ReadString()
	memberID, _ := reader.ReadString()

	s.broker.LeaveGroup(context.Background(), groupID, memberID)

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleSyncGroup(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	_, _ = reader.ReadString()
	_, _ = reader.ReadInt32()
	_, _ = reader.ReadString()
	_, _ = reader.ReadString()
	_, _ = reader.ReadInt32()

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)
	writer.WriteInt32(0)

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleCreateTopics(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	topicCount, _ := reader.ReadInt32()

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	writer.WriteInt32(topicCount)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := reader.ReadString()
		partitions, _ := reader.ReadInt32()
		replication, _ := reader.ReadInt16()

		_ = replication

		writer.WriteInt16(0)
		writer.WriteString(topic)

		s.metadata.CreateTopic(&metadata.TopicMeta{
			Name:       topic,
			Partitions: partitions,
		})
	}

	if apiVersion >= 1 {
		writer.WriteInt32(0)
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleDeleteTopics(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	topicCount, _ := reader.ReadInt32()

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	writer.WriteInt32(topicCount)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := reader.ReadString()
		writer.WriteInt16(0)
		s.metadata.DeleteTopic(topic)
	}

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleInitProducerID(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	_ = reader

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)
	writer.WriteInt64(1)
	writer.WriteInt16(0)

	return writer.Bytes()
}

func (s *KafkaProtocolServer) handleEndTxn(correlationID int32, apiVersion int16, reader *RequestReader) []byte {
	_ = reader

	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(0)

	return writer.Bytes()
}

func (s *KafkaProtocolServer) errorResponse(correlationID int32, apiVersion int16, err error) []byte {
	writer := NewResponseWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(errorCodeFromError(err))
	return writer.Bytes()
}

func errorCodeFromError(err error) int16 {
	switch err {
	case ErrUnknownAPIKey:
		return 35
	case ErrUnsupportedVersion:
		return 35
	case ErrInvalidRequest:
		return 42
	case ErrTopicNotFound:
		return 3
	case ErrPartitionNotFound:
		return 6
	case ErrNotLeaderForPartition:
		return 6
	default:
		return -1
	}
}

func (s *KafkaProtocolServer) Stop() error {
	s.running.Store(false)
	if s.listener != nil {
		s.listener.Close()
	}
	s.clients.Range(func(key, value interface{}) bool {
		if conn, ok := value.(net.Conn); ok {
			conn.Close()
		}
		return true
	})
	return nil
}

type RequestReader struct {
	data   []byte
	offset int
}

func NewRequestReader(data []byte) *RequestReader {
	return &RequestReader{data: data}
}

func (r *RequestReader) ReadInt8() (int8, error) {
	if r.offset+1 > len(r.data) {
		return 0, io.EOF
	}
	v := int8(r.data[r.offset])
	r.offset++
	return v, nil
}

func (r *RequestReader) ReadInt16() (int16, error) {
	if r.offset+2 > len(r.data) {
		return 0, io.EOF
	}
	v := int16(binary.BigEndian.Uint16(r.data[r.offset:]))
	r.offset += 2
	return v, nil
}

func (r *RequestReader) ReadInt32() (int32, error) {
	if r.offset+4 > len(r.data) {
		return 0, io.EOF
	}
	v := int32(binary.BigEndian.Uint32(r.data[r.offset:]))
	r.offset += 4
	return v, nil
}

func (r *RequestReader) ReadInt64() (int64, error) {
	if r.offset+8 > len(r.data) {
		return 0, io.EOF
	}
	v := int64(binary.BigEndian.Uint64(r.data[r.offset:]))
	r.offset += 8
	return v, nil
}

func (r *RequestReader) ReadString() (string, error) {
	len, err := r.ReadInt16()
	if err != nil {
		return "", err
	}
	if len == -1 {
		return "", nil
	}
	if r.offset+int(len) > len(r.data) {
		return "", io.EOF
	}
	v := string(r.data[r.offset : r.offset+int(len)])
	r.offset += int(len)
	return v, nil
}

func (r *RequestReader) ReadBytes() ([]byte, error) {
	len, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if len == -1 {
		return nil, nil
	}
	if r.offset+int(len) > len(r.data) {
		return nil, io.EOF
	}
	v := r.data[r.offset : r.offset+int(len)]
	r.offset += int(len)
	return v, nil
}

func (r *RequestReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

type ResponseWriter struct {
	buf bytes.Buffer
}

func NewResponseWriter() *ResponseWriter {
	return &ResponseWriter{}
}

func (w *ResponseWriter) WriteInt8(v int8) {
	w.buf.WriteByte(byte(v))
}

func (w *ResponseWriter) WriteInt16(v int16) {
	binary.Write(&w.buf, binary.BigEndian, v)
}

func (w *ResponseWriter) WriteInt32(v int32) {
	binary.Write(&w.buf, binary.BigEndian, v)
}

func (w *ResponseWriter) WriteInt64(v int64) {
	binary.Write(&w.buf, binary.BigEndian, v)
}

func (w *ResponseWriter) WriteString(v string) {
	if v == "" {
		w.WriteInt16(-1)
		return
	}
	w.WriteInt16(int16(len(v)))
	w.buf.WriteString(v)
}

func (w *ResponseWriter) WriteBytes(v []byte) {
	if v == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(v)))
	w.buf.Write(v)
}

func (w *ResponseWriter) Write(v []byte) {
	w.buf.Write(v)
}

func (w *ResponseWriter) Bytes() []byte {
	return w.buf.Bytes()
}

func parseRecordBatch(data []byte) [][]byte {
	var records [][]byte
	if len(data) < 61 {
		return records
	}

	offset := 0
	offset += 8
	offset += 4
	offset += 4
	offset += 8
	offset += 4
	offset += 4
	offset += 2
	offset += 4
	offset += 4
	offset += 8
	offset += 8
	offset += 4
	offset += 2
	offset += 4

	recordsCount := int(binary.BigEndian.Uint32(data[offset-4:offset]))

	for i := 0; i < recordsCount && offset < len(data); i++ {
		if offset >= len(data) {
			break
		}

		attributes := data[offset] >> 4
		offset++
		_ = attributes

		timestampDelta := int(data[offset])
		offset++
		_ = timestampDelta

		offsetDelta := int(data[offset])
		offset++
		_ = offsetDelta

		if offset >= len(data) {
			break
		}
		keyLen := int(data[offset])
		offset++
		if keyLen > 0 && offset+keyLen <= len(data) {
			offset += keyLen
		}

		if offset >= len(data) {
			break
		}
		valueLen := int(data[offset])
		offset++
		if valueLen > 0 && offset+valueLen <= len(data) {
			records = append(records, data[offset:offset+valueLen])
			offset += valueLen
		}

		if offset < len(data) {
			headerCount := int(data[offset])
			offset++
			for j := 0; j < headerCount && offset < len(data); j++ {
				if offset < len(data) {
					hKeyLen := int(data[offset])
					offset++
					if hKeyLen > 0 && offset+hKeyLen <= len(data) {
						offset += hKeyLen
					}
				}
				if offset < len(data) {
					hValLen := int(data[offset])
					offset++
					if hValLen > 0 && offset+hValLen <= len(data) {
						offset += hValLen
					}
				}
			}
		}
	}

	return records
}

func buildRecordBatch(messages []*common.Message, baseOffset int64) []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, baseOffset)
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int32(0x22))

	binary.Write(&buf, binary.BigEndian, int64(0))
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int16(0))
	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, int32(0))

	binary.Write(&buf, binary.BigEndian, int64(0))
	binary.Write(&buf, binary.BigEndian, int64(0))

	binary.Write(&buf, binary.BigEndian, int32(0x22))
	binary.Write(&buf, binary.BigEndian, int16(2))
	binary.Write(&buf, binary.BigEndian, int32(int32(len(messages))))

	for i, msg := range messages {
		buf.WriteByte(0)

		buf.WriteByte(0)
		buf.WriteByte(byte(i))

		buf.WriteByte(0)

		buf.WriteByte(byte(len(msg.Value)))
		buf.Write(msg.Value)

		buf.WriteByte(0)
	}

	return buf.Bytes()
}

func parseInt32(s string) int32 {
	v, _ := strconv.ParseInt(s, 10, 32)
	return int32(v)
}
