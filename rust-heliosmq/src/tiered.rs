use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageClass {
    Hot,
    Warm,
    Cold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredStorageConfig {
    pub enabled: bool,
    pub provider: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub hot_tier_path: String,
    pub hot_tier_size_gb: u64,
    pub cold_tier_days: u64,
    pub max_upload_workers: usize,
    pub max_download_conns: usize,
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "s3".to_string(),
            endpoint: String::new(),
            region: "us-east-1".to_string(),
            bucket: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            hot_tier_path: "./data/hot".to_string(),
            hot_tier_size_gb: 100,
            cold_tier_days: 7,
            max_upload_workers: 10,
            max_download_conns: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentManifest {
    pub topic: String,
    pub partition: i32,
    pub base_offset: i64,
    pub s3_key: String,
    pub size: u64,
    pub etag: String,
    pub uploaded_at: i64,
    pub storage_class: StorageClass,
}

pub struct TieredStorage {
    config: TieredStorageConfig,
    hot_tier: HotTierManager,
    cold_tier: ColdTierManager,
    manifest: ManifestManager,
    uploader: Uploader,
    downloader: Downloader,
    running: RwLock<bool>,
}

impl TieredStorage {
    pub fn new(config: TieredStorageConfig) -> anyhow::Result<Self> {
        if !config.enabled {
            return Ok(Self {
                config,
                hot_tier: HotTierManager::new("", 0),
                cold_tier: ColdTierManager::new(String::new(), String::new()),
                manifest: ManifestManager::new(String::new(), String::new()),
                uploader: Uploader::new(String::new(), 0),
                downloader: Downloader::new(String::new(), 0),
                running: RwLock::new(false),
            });
        }

        let hot_tier = HotTierManager::new(&config.hot_tier_path, config.hot_tier_size_gb);
        let cold_tier = ColdTierManager::new(config.bucket.clone(), config.endpoint.clone());
        let manifest = ManifestManager::new(config.bucket.clone(), config.endpoint.clone());
        let uploader = Uploader::new(config.bucket.clone(), config.max_upload_workers);
        let downloader = Downloader::new(config.bucket.clone(), config.max_download_conns);

        Ok(Self {
            config,
            hot_tier,
            cold_tier,
            manifest,
            uploader,
            downloader,
            running: RwLock::new(true),
        })
    }

    pub async fn upload_segment(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let key = self.segment_key(topic, partition, base_offset);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let metadata = HashMap::from([
            ("topic".to_string(), topic.to_string()),
            ("partition".to_string(), partition.to_string()),
            ("base-offset".to_string(), base_offset.to_string()),
            ("uploaded-at".to_string(), now.to_string()),
            ("size".to_string(), data.len().to_string()),
        ]);

        let result = self.uploader.upload(&key, data, metadata).await?;

        let manifest = SegmentManifest {
            topic: topic.to_string(),
            partition,
            base_offset,
            s3_key: key,
            size: data.len() as u64,
            etag: result.etag,
            uploaded_at: now,
            storage_class: StorageClass::Cold,
        };

        self.manifest.record_segment(&manifest).await?;

        Ok(())
    }

    pub async fn download_segment(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
    ) -> anyhow::Result<Vec<u8>> {
        if !self.config.enabled {
            return Err(anyhow::anyhow!("Tiered storage not enabled"));
        }

        let manifest = self.manifest.get_segment(topic, partition, base_offset).await?
            .ok_or_else(|| anyhow::anyhow!("Segment not found in manifest"))?;

        let data = self.downloader.download(&manifest.s3_key).await?;

        Ok(data)
    }

    pub async fn delete_segment(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
    ) -> anyhow::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let manifest = self.manifest.get_segment(topic, partition, base_offset).await?;
        if let Some(m) = manifest {
            self.cold_tier.delete_object(&m.s3_key).await?;
            self.manifest.delete_segment(topic, partition, base_offset).await?;
        }

        Ok(())
    }

    pub async fn list_segments(&self, topic: &str, partition: i32) -> anyhow::Result<Vec<SegmentManifest>> {
        self.manifest.list_segments(topic, partition).await
    }

    pub fn get_storage_class(&self, topic: &str, partition: i32, base_offset: i64) -> StorageClass {
        if self.hot_tier.exists(topic, partition, base_offset) {
            return StorageClass::Hot;
        }

        StorageClass::Cold
    }

    pub fn start_background_tiering(&self) {
        let running = self.running.clone();
        let cold_tier_days = self.config.cold_tier_days;
        let hot_tier = self.hot_tier.clone();
        let uploader = self.uploader.clone();
        let manifest = self.manifest.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));

            loop {
                interval.tick().await;

                if !*running.read() {
                    break;
                }

                let cutoff = Instant::now() - Duration::from_secs(cold_tier_days * 86400);
                let segments = hot_tier.list_old_segments(cutoff);

                for seg in segments {
                    if let Ok(data) = hot_tier.read_segment(&seg.topic, seg.partition, seg.base_offset) {
                        let key = format!("segments/{}/{}/{:020}.seg", seg.topic, seg.partition, seg.base_offset);
                        if uploader.upload(&key, &data, HashMap::new()).await.is_ok() {
                            hot_tier.delete_segment(&seg.topic, seg.partition, seg.base_offset);
                        }
                    }
                }
            }
        });
    }

    fn segment_key(&self, topic: &str, partition: i32, base_offset: i64) -> String {
        format!("segments/{}/{}/{:020}.seg", topic, partition, base_offset)
    }

    pub fn close(&self) {
        *self.running.write() = false;
    }
}

#[derive(Debug, Clone)]
pub struct HotTierManager {
    base_path: PathBuf,
    max_size_gb: u64,
    current_size: RwLock<u64>,
}

#[derive(Debug, Clone)]
pub struct HotSegment {
    pub topic: String,
    pub partition: i32,
    pub base_offset: i64,
    pub size: u64,
    pub last_access: Instant,
}

impl HotTierManager {
    pub fn new(base_path: &str, max_size_gb: u64) -> Self {
        Self {
            base_path: PathBuf::from(base_path),
            max_size_gb,
            current_size: RwLock::new(0),
        }
    }

    pub fn exists(&self, topic: &str, partition: i32, base_offset: i64) -> bool {
        let path = self.segment_path(topic, partition, base_offset);
        path.exists()
    }

    pub fn read_segment(&self, topic: &str, partition: i32, base_offset: i64) -> anyhow::Result<Vec<u8>> {
        let path = self.segment_path(topic, partition, base_offset);
        Ok(std::fs::read(path)?)
    }

    pub fn write_segment(&self, topic: &str, partition: i32, base_offset: i64, data: &[u8]) -> anyhow::Result<()> {
        let path = self.segment_path(topic, partition, base_offset);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data)?;
        *self.current_size.write() += data.len() as u64;
        Ok(())
    }

    pub fn delete_segment(&self, topic: &str, partition: i32, base_offset: i64) {
        let path = self.segment_path(topic, partition, base_offset);
        let _ = std::fs::remove_file(path);
    }

    pub fn list_old_segments(&self, cutoff: Instant) -> Vec<HotSegment> {
        Vec::new()
    }

    fn segment_path(&self, topic: &str, partition: i32, base_offset: i64) -> PathBuf {
        self.base_path
            .join(topic)
            .join(format!("partition-{}", partition))
            .join(format!("{:020}.seg", base_offset))
    }
}

#[derive(Debug, Clone)]
pub struct ColdTierManager {
    bucket: String,
    endpoint: String,
}

impl ColdTierManager {
    pub fn new(bucket: String, endpoint: String) -> Self {
        Self { bucket, endpoint }
    }

    pub async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ManifestManager {
    bucket: String,
    endpoint: String,
    cache: RwLock<HashMap<String, SegmentManifest>>,
}

impl ManifestManager {
    pub fn new(bucket: String, endpoint: String) -> Self {
        Self {
            bucket,
            endpoint,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn record_segment(&self, manifest: &SegmentManifest) -> anyhow::Result<()> {
        let key = self.manifest_key(&manifest.topic, manifest.partition, manifest.base_offset);
        self.cache.write().insert(key, manifest.clone());
        Ok(())
    }

    pub async fn get_segment(&self, topic: &str, partition: i32, base_offset: i64) -> anyhow::Result<Option<SegmentManifest>> {
        let key = self.manifest_key(topic, partition, base_offset);
        Ok(self.cache.read().get(&key).cloned())
    }

    pub async fn delete_segment(&self, topic: &str, partition: i32, base_offset: i64) -> anyhow::Result<()> {
        let key = self.manifest_key(topic, partition, base_offset);
        self.cache.write().remove(&key);
        Ok(())
    }

    pub async fn list_segments(&self, topic: &str, partition: i32) -> anyhow::Result<Vec<SegmentManifest>> {
        let prefix = format!("manifest/{}/{}/", topic, partition);
        let cache = self.cache.read();
        let segments: Vec<_> = cache
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect();
        Ok(segments)
    }

    fn manifest_key(&self, topic: &str, partition: i32, base_offset: i64) -> String {
        format!("manifest/{}/{}/{:020}.manifest", topic, partition, base_offset)
    }
}

#[derive(Debug, Clone)]
pub struct UploadResult {
    pub etag: String,
    pub version_id: Option<String>,
    pub location: String,
}

#[derive(Debug, Clone)]
pub struct Uploader {
    bucket: String,
    workers: usize,
}

impl Uploader {
    pub fn new(bucket: String, workers: usize) -> Self {
        Self { bucket, workers }
    }

    pub async fn upload(&self, key: &str, data: &[u8], metadata: HashMap<String, String>) -> anyhow::Result<UploadResult> {
        Ok(UploadResult {
            etag: format!("{:x}", md5_hash(data)),
            version_id: None,
            location: format!("https://{}.s3.amazonaws.com/{}", self.bucket, key),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Downloader {
    bucket: String,
    max_conns: usize,
}

impl Downloader {
    pub fn new(bucket: String, max_conns: usize) -> Self {
        Self { bucket, max_conns }
    }

    pub async fn download(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        Ok(Vec::new())
    }
}

fn md5_hash(data: &[u8]) -> u64 {
    let mut hash: u64 = 0;
    for chunk in data.chunks(8) {
        let mut arr = [0u8; 8];
        arr[..chunk.len()].copy_from_slice(chunk);
        hash = hash.wrapping_add(u64::from_le_bytes(arr));
    }
    hash
}
