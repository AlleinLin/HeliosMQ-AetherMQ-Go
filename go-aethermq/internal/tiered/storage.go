package tiered

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	ErrObjectNotFound = errors.New("object not found")
	ErrUploadFailed   = errors.New("upload failed")
	ErrDownloadFailed = errors.New("download failed")
)

type StorageClass string

const (
	StorageClassHot  StorageClass = "HOT"
	StorageClassWarm StorageClass = "WARM"
	StorageClassCold StorageClass = "COLD"
)

type TieredStorageConfig struct {
	Enabled          bool
	Provider         string
	Endpoint         string
	Region           string
	Bucket           string
	AccessKey        string
	SecretKey        string
	HotTierPath      string
	HotTierSizeGB    int64
	ColdTierDays     int64
	MaxUploadWorkers int
	MaxDownloadConns int
}

type TieredStorage struct {
	config    *TieredStorageConfig
	s3Client  *s3.Client
	hotTier   *HotTierManager
	coldTier  *ColdTierManager
	manifest  *ManifestManager
	uploader  *Uploader
	downloader *Downloader
	mu        sync.RWMutex
	running   bool
}

func NewTieredStorage(cfg *TieredStorageConfig) (*TieredStorage, error) {
	if !cfg.Enabled {
		return &TieredStorage{config: cfg}, nil
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3Opts...)

	hotTier, err := NewHotTierManager(cfg.HotTierPath, cfg.HotTierSizeGB)
	if err != nil {
		return nil, fmt.Errorf("failed to create hot tier manager: %w", err)
	}

	coldTier := NewColdTierManager(s3Client, cfg.Bucket)
	manifest := NewManifestManager(s3Client, cfg.Bucket)
	uploader := NewUploader(s3Client, cfg.Bucket, cfg.MaxUploadWorkers)
	downloader := NewDownloader(s3Client, cfg.Bucket, cfg.MaxDownloadConns)

	ts := &TieredStorage{
		config:     cfg,
		s3Client:   s3Client,
		hotTier:    hotTier,
		coldTier:   coldTier,
		manifest:   manifest,
		uploader:   uploader,
		downloader: downloader,
		running:    true,
	}

	go ts.backgroundTiering()

	return ts, nil
}

func (ts *TieredStorage) UploadSegment(ctx context.Context, topic string, partition int32, baseOffset int64, data []byte) error {
	if !ts.config.Enabled {
		return nil
	}

	key := ts.segmentKey(topic, partition, baseOffset)
	metadata := map[string]string{
		"topic":       topic,
		"partition":   fmt.Sprintf("%d", partition),
		"base-offset": fmt.Sprintf("%d", baseOffset),
		"uploaded-at": time.Now().UTC().Format(time.RFC3339),
		"size":        fmt.Sprintf("%d", len(data)),
	}

	result, err := ts.uploader.Upload(ctx, key, data, metadata)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}

	if err := ts.manifest.RecordSegment(ctx, &SegmentManifest{
		Topic:       topic,
		Partition:   partition,
		BaseOffset:  baseOffset,
		S3Key:       key,
		Size:        int64(len(data)),
		ETag:        result.ETag,
		UploadedAt:  time.Now().UnixMilli(),
		StorageClass: string(StorageClassCold),
	}); err != nil {
		return fmt.Errorf("failed to record manifest: %w", err)
	}

	return nil
}

func (ts *TieredStorage) DownloadSegment(ctx context.Context, topic string, partition int32, baseOffset int64) ([]byte, error) {
	if !ts.config.Enabled {
		return nil, errors.New("tiered storage not enabled")
	}

	manifest, err := ts.manifest.GetSegment(ctx, topic, partition, baseOffset)
	if err != nil {
		return nil, fmt.Errorf("segment not found in manifest: %w", err)
	}

	data, err := ts.downloader.Download(ctx, manifest.S3Key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}

	return data, nil
}

func (ts *TieredStorage) DeleteSegment(ctx context.Context, topic string, partition int32, baseOffset int64) error {
	if !ts.config.Enabled {
		return nil
	}

	manifest, err := ts.manifest.GetSegment(ctx, topic, partition, baseOffset)
	if err != nil {
		return nil
	}

	if _, err := ts.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(ts.config.Bucket),
		Key:    aws.String(manifest.S3Key),
	}); err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return ts.manifest.DeleteSegment(ctx, topic, partition, baseOffset)
}

func (ts *TieredStorage) ListSegments(ctx context.Context, topic string, partition int32) ([]*SegmentManifest, error) {
	return ts.manifest.ListSegments(ctx, topic, partition)
}

func (ts *TieredStorage) GetStorageClass(topic string, partition int32, baseOffset int64) (StorageClass, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if ts.hotTier.Exists(topic, partition, baseOffset) {
		return StorageClassHot, nil
	}

	ctx := context.Background()
	if _, err := ts.manifest.GetSegment(ctx, topic, partition, baseOffset); err == nil {
		return StorageClassCold, nil
	}

	return StorageClassHot, nil
}

func (ts *TieredStorage) backgroundTiering() {
	if !ts.config.Enabled || ts.config.ColdTierDays <= 0 {
		return
	}

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		if !ts.running {
			return
		}
		ts.tierColdSegments()
	}
}

func (ts *TieredStorage) tierColdSegments() {
	ctx := context.Background()
	cutoff := time.Now().Add(-time.Duration(ts.config.ColdTierDays) * 24 * time.Hour)

	segments := ts.hotTier.ListOldSegments(cutoff)
	for _, seg := range segments {
		data, err := ts.hotTier.ReadSegment(seg.Topic, seg.Partition, seg.BaseOffset)
		if err != nil {
			continue
		}

		if err := ts.UploadSegment(ctx, seg.Topic, seg.Partition, seg.BaseOffset, data); err != nil {
			continue
		}

		ts.hotTier.DeleteSegment(seg.Topic, seg.Partition, seg.BaseOffset)
	}
}

func (ts *TieredStorage) segmentKey(topic string, partition int32, baseOffset int64) string {
	return fmt.Sprintf("segments/%s/%d/%020d.seg", topic, partition, baseOffset)
}

func (ts *TieredStorage) Close() error {
	ts.running = false
	return ts.hotTier.Close()
}

type HotTierManager struct {
	basePath    string
	maxSizeGB   int64
	currentSize int64
	mu          sync.RWMutex
}

type HotSegment struct {
	Topic       string
	Partition   int32
	BaseOffset  int64
	Size        int64
	LastAccess  time.Time
}

func NewHotTierManager(basePath string, maxSizeGB int64) (*HotTierManager, error) {
	return &HotTierManager{
		basePath:  basePath,
		maxSizeGB: maxSizeGB,
	}, nil
}

func (h *HotTierManager) Exists(topic string, partition int32, baseOffset int64) bool {
	path := h.segmentPath(topic, partition, baseOffset)
	_, err := filepath.Abs(path)
	return err == nil
}

func (h *HotTierManager) ReadSegment(topic string, partition int32, baseOffset int64) ([]byte, error) {
	path := h.segmentPath(topic, partition, baseOffset)
	data, err := readFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (h *HotTierManager) WriteSegment(topic string, partition int32, baseOffset int64, data []byte) error {
	path := h.segmentPath(topic, partition, baseOffset)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.currentSize += int64(len(data))
	return writeFile(path, data)
}

func (h *HotTierManager) DeleteSegment(topic string, partition int32, baseOffset int64) error {
	path := h.segmentPath(topic, partition, baseOffset)
	h.mu.Lock()
	defer h.mu.Unlock()
	return deleteFile(path)
}

func (h *HotTierManager) ListOldSegments(cutoff time.Time) []*HotSegment {
	return []*HotSegment{}
}

func (h *HotTierManager) segmentPath(topic string, partition int32, baseOffset int64) string {
	return filepath.Join(h.basePath, topic, fmt.Sprintf("partition-%d", partition), fmt.Sprintf("%020d.seg", baseOffset))
}

func (h *HotTierManager) Close() error {
	return nil
}

type ColdTierManager struct {
	s3Client *s3.Client
	bucket   string
}

func NewColdTierManager(s3Client *s3.Client, bucket string) *ColdTierManager {
	return &ColdTierManager{
		s3Client: s3Client,
		bucket:   bucket,
	}
}

type ManifestManager struct {
	s3Client *s3.Client
	bucket   string
	cache    sync.Map
}

type SegmentManifest struct {
	Topic        string `json:"topic"`
	Partition    int32  `json:"partition"`
	BaseOffset   int64  `json:"base_offset"`
	S3Key        string `json:"s3_key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	UploadedAt   int64  `json:"uploaded_at"`
	StorageClass string `json:"storage_class"`
}

func NewManifestManager(s3Client *s3.Client, bucket string) *ManifestManager {
	return &ManifestManager{
		s3Client: s3Client,
		bucket:   bucket,
	}
}

func (m *ManifestManager) RecordSegment(ctx context.Context, manifest *SegmentManifest) error {
	key := m.manifestKey(manifest.Topic, manifest.Partition, manifest.BaseOffset)
	data, err := encodeManifest(manifest)
	if err != nil {
		return err
	}

	m.cache.Store(key, manifest)

	_, err = m.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (m *ManifestManager) GetSegment(ctx context.Context, topic string, partition int32, baseOffset int64) (*SegmentManifest, error) {
	key := m.manifestKey(topic, partition, baseOffset)

	if cached, ok := m.cache.Load(key); ok {
		return cached.(*SegmentManifest), nil
	}

	resp, err := m.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	manifest, err := decodeManifest(data)
	if err != nil {
		return nil, err
	}

	m.cache.Store(key, manifest)
	return manifest, nil
}

func (m *ManifestManager) DeleteSegment(ctx context.Context, topic string, partition int32, baseOffset int64) error {
	key := m.manifestKey(topic, partition, baseOffset)
	m.cache.Delete(key)

	_, err := m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (m *ManifestManager) ListSegments(ctx context.Context, topic string, partition int32) ([]*SegmentManifest, error) {
	prefix := fmt.Sprintf("manifest/%s/%d/", topic, partition)
	var manifests []*SegmentManifest

	paginator := s3.NewListObjectsV2Paginator(m.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(m.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".manifest") {
				resp, err := m.s3Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(m.bucket),
					Key:    obj.Key,
				})
				if err != nil {
					continue
				}

				data, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					continue
				}

				manifest, err := decodeManifest(data)
				if err != nil {
					continue
				}
				manifests = append(manifests, manifest)
			}
		}
	}

	return manifests, nil
}

func (m *ManifestManager) manifestKey(topic string, partition int32, baseOffset int64) string {
	return fmt.Sprintf("manifest/%s/%d/%020d.manifest", topic, partition, baseOffset)
}

type Uploader struct {
	s3Client *s3.Client
	bucket   string
	workers  int
	pool     chan struct{}
}

type UploadResult struct {
	ETag       string
	VersionID  string
	Location   string
}

func NewUploader(s3Client *s3.Client, bucket string, workers int) *Uploader {
	if workers <= 0 {
		workers = 10
	}
	return &Uploader{
		s3Client: s3Client,
		bucket:   bucket,
		workers:  workers,
		pool:     make(chan struct{}, workers),
	}
}

func (u *Uploader) Upload(ctx context.Context, key string, data []byte, metadata map[string]string) (*UploadResult, error) {
	u.pool <- struct{}{}
	defer func() { <-u.pool }()

	var meta map[string]string
	if metadata != nil {
		meta = metadata
	}

	resp, err := u.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:     aws.String(u.bucket),
		Key:        aws.String(key),
		Body:       bytes.NewReader(data),
		Metadata:   meta,
	})
	if err != nil {
		return nil, err
	}

	result := &UploadResult{}
	if resp.ETag != nil {
		result.ETag = *resp.ETag
	}
	if resp.VersionId != nil {
		result.VersionID = *resp.VersionId
	}
	result.Location = fmt.Sprintf("https://%s.s3.amazonaws.com/%s", u.bucket, key)

	return result, nil
}

type Downloader struct {
	s3Client *s3.Client
	bucket   string
	maxConns int
}

func NewDownloader(s3Client *s3.Client, bucket string, maxConns int) *Downloader {
	if maxConns <= 0 {
		maxConns = 20
	}
	return &Downloader{
		s3Client: s3Client,
		bucket:   bucket,
		maxConns: maxConns,
	}
}

func (d *Downloader) Download(ctx context.Context, key string) ([]byte, error) {
	resp, err := d.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func encodeManifest(m *SegmentManifest) ([]byte, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, []byte(m.Topic)...)
	buf = append(buf, 0)
	buf = append(buf, int32ToBytes(m.Partition)...)
	buf = append(buf, int64ToBytes(m.BaseOffset)...)
	buf = append(buf, []byte(m.S3Key)...)
	buf = append(buf, 0)
	buf = append(buf, int64ToBytes(m.Size)...)
	buf = append(buf, []byte(m.ETag)...)
	buf = append(buf, 0)
	buf = append(buf, int64ToBytes(m.UploadedAt)...)
	buf = append(buf, []byte(m.StorageClass)...)
	return buf, nil
}

func decodeManifest(data []byte) (*SegmentManifest, error) {
	parts := splitNull(data)
	if len(parts) < 7 {
		return nil, errors.New("invalid manifest format")
	}
	return &SegmentManifest{
		Topic:        string(parts[0]),
		Partition:    bytesToInt32(parts[1]),
		BaseOffset:   bytesToInt64(parts[2]),
		S3Key:        string(parts[3]),
		Size:         bytesToInt64(parts[4]),
		ETag:         string(parts[5]),
		UploadedAt:   bytesToInt64(parts[6]),
		StorageClass: string(parts[7]),
	}, nil
}

func splitNull(data []byte) [][]byte {
	var parts [][]byte
	start := 0
	for i, b := range data {
		if b == 0 {
			parts = append(parts, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		parts = append(parts, data[start:])
	}
	return parts
}

func int32ToBytes(n int32) []byte {
	return []byte{
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	}
}

func bytesToInt32(b []byte) int32 {
	if len(b) < 4 {
		return 0
	}
	return int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
}

func int64ToBytes(n int64) []byte {
	return []byte{
		byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	}
}

func bytesToInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
		int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
}

func readFile(path string) ([]byte, error) {
	return []byte{}, nil
}

func writeFile(path string, data []byte) error {
	return nil
}

func deleteFile(path string) error {
	return nil
}
