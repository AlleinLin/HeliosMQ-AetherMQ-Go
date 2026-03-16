package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aethermq/aethermq/internal/common"
	"github.com/klauspost/compress/zstd"
)

const (
	DefaultSegmentSize   = 1 << 30
	IndexInterval        = 4096
	HeaderSize           = 28
	MagicNumber   uint32 = 0xAEAEAEAE
)

type SegmentStore struct {
	mu          sync.RWMutex
	baseDir     string
	segmentSize int64
	segments    map[string]*PartitionLog
	compressor  *zstd.Encoder
	decompressor *zstd.Decoder
}

type PartitionLog struct {
	mu           sync.RWMutex
	topic        string
	partition    int32
	baseDir      string
	segments     []*Segment
	activeSegment *Segment
	leo          int64
	hw           int64
}

type Segment struct {
	mu          sync.Mutex
	baseOffset  int64
	file        *os.File
	writer      *bufio.Writer
	logPath     string
	indexPath   string
	timeIdxPath string
	index       *OffsetIndex
	timeIndex   *TimeIndex
	size        int64
	sealed      bool
	createTime  time.Time
}

type OffsetIndex struct {
	file   *os.File
	entries []IndexEntry
}

type TimeIndex struct {
	file    *os.File
	entries []TimeIndexEntry
}

type IndexEntry struct {
	Offset   int64
	Position int64
}

type TimeIndexEntry struct {
	Timestamp int64
	Offset    int64
}

func NewSegmentStore(baseDir string, segmentSize int64) (*SegmentStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	encoder, _ := zstd.NewWriter(nil)
	decoder, _ := zstd.NewReader(nil)
	return &SegmentStore{
		baseDir:     baseDir,
		segmentSize: segmentSize,
		segments:    make(map[string]*PartitionLog),
		compressor:  encoder,
		decompressor: decoder,
	}, nil
}

func (s *SegmentStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, log := range s.segments {
		log.Close()
	}
	return nil
}

func (s *SegmentStore) Append(msg *common.Message) (int64, error) {
	log, err := s.getOrCreatePartitionLog(msg.Topic, msg.Partition)
	if err != nil {
		return -1, err
	}
	return log.Append(msg)
}

func (s *SegmentStore) Fetch(topic string, partition int32, offset int64, maxBytes int32) ([]*common.Message, int64, error) {
	log, err := s.getOrCreatePartitionLog(topic, partition)
	if err != nil {
		return nil, -1, err
	}
	return log.Fetch(offset, maxBytes)
}

func (s *SegmentStore) getOrCreatePartitionLog(topic string, partition int32) (*PartitionLog, error) {
	key := partitionKey(topic, partition)
	s.mu.RLock()
	if log, ok := s.segments[key]; ok {
		s.mu.RUnlock()
		return log, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if log, ok := s.segments[key]; ok {
		return log, nil
	}
	log, err := NewPartitionLog(s.baseDir, topic, partition, s.segmentSize)
	if err != nil {
		return nil, err
	}
	s.segments[key] = log
	return log, nil
}

func NewPartitionLog(baseDir, topic string, partition int32, segmentSize int64) (*PartitionLog, error) {
	dir := filepath.Join(baseDir, topic, fmt.Sprintf("partition-%d", partition))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	pl := &PartitionLog{
		topic:       topic,
		partition:   partition,
		baseDir:     dir,
		segments:    make([]*Segment, 0),
		segmentSize: segmentSize,
	}
	if err := pl.loadExistingSegments(); err != nil {
		return nil, err
	}
	if len(pl.segments) == 0 {
		seg, err := pl.createNewSegment(0)
		if err != nil {
			return nil, err
		}
		pl.segments = append(pl.segments, seg)
		pl.activeSegment = seg
	} else {
		pl.activeSegment = pl.segments[len(pl.segments)-1]
		pl.leo = pl.activeSegment.baseOffset + pl.activeSegment.size
	}
	return pl, nil
}

func (pl *PartitionLog) loadExistingSegments() error {
	entries, err := os.ReadDir(pl.baseDir)
	if err != nil {
		return err
	}
	var logFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".log" {
			logFiles = append(logFiles, entry.Name())
		}
	}
	sort.Strings(logFiles)
	for _, name := range logFiles {
		var baseOffset int64
		fmt.Sscanf(name, "%d.log", &baseOffset)
		seg, err := pl.openSegment(baseOffset)
		if err != nil {
			return err
		}
		pl.segments = append(pl.segments, seg)
	}
	return nil
}

func (pl *PartitionLog) createNewSegment(baseOffset int64) (*Segment, error) {
	logPath := filepath.Join(pl.baseDir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(pl.baseDir, fmt.Sprintf("%020d.index", baseOffset))
	timeIdxPath := filepath.Join(pl.baseDir, fmt.Sprintf("%020d.timeindex", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	idxFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	timeIdxFile, err := os.OpenFile(timeIdxPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		idxFile.Close()
		return nil, err
	}

	stat, _ := logFile.Stat()
	seg := &Segment{
		baseOffset:  baseOffset,
		file:        logFile,
		writer:      bufio.NewWriterSize(logFile, 1<<20),
		logPath:     logPath,
		indexPath:   indexPath,
		timeIdxPath: timeIdxPath,
		index:       &OffsetIndex{file: idxFile},
		timeIndex:   &TimeIndex{file: timeIdxFile},
		size:        stat.Size(),
		createTime:  time.Now(),
	}
	return seg, nil
}

func (pl *PartitionLog) openSegment(baseOffset int64) (*Segment, error) {
	return pl.createNewSegment(baseOffset)
}

func (pl *PartitionLog) Append(msg *common.Message) (int64, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if pl.activeSegment.size > pl.segmentSize {
		if err := pl.activeSegment.Seal(); err != nil {
			return -1, err
		}
		newSeg, err := pl.createNewSegment(pl.leo)
		if err != nil {
			return -1, err
		}
		pl.segments = append(pl.segments, newSeg)
		pl.activeSegment = newSeg
	}

	offset := pl.leo
	msg.Offset = offset
	msg.Partition = pl.partition

	data, err := encodeMessage(msg)
	if err != nil {
		return -1, err
	}

	if err := pl.activeSegment.Append(data, offset, msg.Timestamp); err != nil {
		return -1, err
	}
	pl.leo++
	return offset, nil
}

func (pl *PartitionLog) Fetch(offset int64, maxBytes int32) ([]*common.Message, int64, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	if offset >= pl.leo {
		return nil, pl.leo, nil
	}

	var seg *Segment
	for _, s := range pl.segments {
		if offset >= s.baseOffset && offset < s.baseOffset+s.size {
			seg = s
			break
		}
	}
	if seg == nil {
		return nil, pl.leo, errors.New("offset out of range")
	}

	messages, err := seg.Read(offset, maxBytes)
	return messages, pl.leo, err
}

func (pl *PartitionLog) Close() error {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	for _, seg := range pl.segments {
		seg.Close()
	}
	return nil
}

func (s *Segment) Append(data []byte, offset int64, timestamp int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)

	n, err := s.writer.Write(buf)
	if err != nil {
		return err
	}
	s.size += int64(n)

	if (offset-s.baseOffset)%IndexInterval == 0 {
		s.index.entries = append(s.index.entries, IndexEntry{
			Offset:   offset,
			Position: s.size - int64(n),
		})
		entry := make([]byte, 16)
		binary.BigEndian.PutUint64(entry[:8], uint64(offset))
		binary.BigEndian.PutUint64(entry[8:], uint64(s.size-int64(n)))
		s.index.file.Write(entry)
	}

	s.timeIndex.entries = append(s.timeIndex.entries, TimeIndexEntry{
		Timestamp: timestamp,
		Offset:    offset,
	})

	return nil
}

func (s *Segment) Read(offset int64, maxBytes int32) ([]*common.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writer.Flush(); err != nil {
		return nil, err
	}

	pos := s.findPosition(offset)
	if _, err := s.file.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	var messages []*common.Message
	var bytesRead int32

	for bytesRead < maxBytes {
		var length uint32
		if err := binary.Read(s.file, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(s.file, data); err != nil {
			return nil, err
		}
		msg, err := decodeMessage(data)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
		bytesRead += int32(4 + length)
	}
	return messages, nil
}

func (s *Segment) findPosition(offset int64) int64 {
	entries := s.index.entries
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].Offset >= offset
	})
	if idx > 0 {
		return entries[idx-1].Position
	}
	return 0
}

func (s *Segment) Seal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sealed = true
	return s.writer.Flush()
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writer.Flush(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	if err := s.index.file.Close(); err != nil {
		return err
	}
	if err := s.timeIndex.file.Close(); err != nil {
		return err
	}
	return nil
}

func encodeMessage(msg *common.Message) ([]byte, error) {
	size := 8 + 4 + len(msg.Topic) + 4 + len(msg.Key) + 4 + len(msg.Value) + 8 + 8 + 4
	for k, v := range msg.Headers {
		size += 4 + len(k) + 4 + len(v)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, int64ToBytes(msg.Offset)...)
	buf = append(buf, int32ToBytes(int32(len(msg.Topic)))...)
	buf = append(buf, []byte(msg.Topic)...)
	buf = append(buf, int32ToBytes(int32(len(msg.Key)))...)
	buf = append(buf, msg.Key...)
	buf = append(buf, int32ToBytes(int32(len(msg.Value)))...)
	buf = append(buf, msg.Value...)
	buf = append(buf, int64ToBytes(msg.Timestamp)...)
	buf = append(buf, int64ToBytes(msg.DeliverAt)...)
	buf = append(buf, int32ToBytes(int32(len(msg.Headers)))...)
	for k, v := range msg.Headers {
		buf = append(buf, int32ToBytes(int32(len(k)))...)
		buf = append(buf, []byte(k)...)
		buf = append(buf, int32ToBytes(int32(len(v)))...)
		buf = append(buf, []byte(v)...)
	}
	return buf, nil
}

func decodeMessage(data []byte) (*common.Message, error) {
	msg := &common.Message{Headers: make(map[string]string)}
	offset := 0
	msg.Offset = bytesToInt64(data[offset : offset+8])
	offset += 8
	topicLen := bytesToInt32(data[offset : offset+4])
	offset += 4
	msg.Topic = string(data[offset : offset+int(topicLen)])
	offset += int(topicLen)
	keyLen := bytesToInt32(data[offset : offset+4])
	offset += 4
	msg.Key = data[offset : offset+int(keyLen)]
	offset += int(keyLen)
	valueLen := bytesToInt32(data[offset : offset+4])
	offset += 4
	msg.Value = data[offset : offset+int(valueLen)]
	offset += int(valueLen)
	msg.Timestamp = bytesToInt64(data[offset : offset+8])
	offset += 8
	msg.DeliverAt = bytesToInt64(data[offset : offset+8])
	offset += 8
	headerCount := bytesToInt32(data[offset : offset+4])
	offset += 4
	for i := 0; i < int(headerCount); i++ {
		keyLen := bytesToInt32(data[offset : offset+4])
		offset += 4
		key := string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)
		valueLen := bytesToInt32(data[offset : offset+4])
		offset += 4
		value := string(data[offset : offset+int(valueLen)])
		offset += int(valueLen)
		msg.Headers[key] = value
	}
	return msg, nil
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func int64ToBytes(n int64) []byte {
	return []byte{
		byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	}
}

func bytesToInt64(b []byte) int64 {
	return int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
		int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
}

func int32ToBytes(n int32) []byte {
	return []byte{
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	}
}

func bytesToInt32(b []byte) int32 {
	return int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
}
