use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::collections::BTreeMap;
use std::io::{self, Seek, SeekFrom, Read, Write, BufWriter};
use std::fs::{self, File, OpenOptions};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use bytes::Bytes;
use memmap2::MmapMut;

use crate::common::Message;
use crate::config::BrokerConfig;

pub const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;
pub const INDEX_INTERVAL: u64 = 4096;
pub const HEADER_SIZE: usize = 28;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Segment not found")]
    SegmentNotFound,
    #[error("Offset out of range")]
    OffsetOutOfRange,
    #[error("Corrupted segment")]
    CorruptedSegment,
    #[error("Compression error: {0}")]
    Compression(String),
}

pub struct SegmentStore {
    config: Arc<BrokerConfig>,
    partitions: RwLock<BTreeMap<String, Arc<PartitionLog>>>,
}

impl SegmentStore {
    pub fn open(config: &BrokerConfig) -> anyhow::Result<Self> {
        let data_dir = Path::new(&config.data_dir);
        fs::create_dir_all(data_dir)?;

        let store = Self {
            config: Arc::new(config.clone()),
            partitions: RwLock::new(BTreeMap::new()),
        };

        store.load_existing_partitions()?;
        Ok(store)
    }

    fn load_existing_partitions(&self) -> anyhow::Result<()> {
        let data_dir = Path::new(&self.config.data_dir);
        if !data_dir.exists() {
            return Ok(());
        }

        for topic_entry in fs::read_dir(data_dir)? {
            let topic_entry = topic_entry?;
            if !topic_entry.file_type()?.is_dir() {
                continue;
            }

            let topic = topic_entry.file_name().to_string_lossy().to_string();
            for partition_entry in fs::read_dir(topic_entry.path())? {
                let partition_entry = partition_entry?;
                let partition_name = partition_entry.file_name().to_string_lossy().to_string();
                
                if let Some(partition_str) = partition_name.strip_prefix("partition-") {
                    if let Ok(partition) = partition_str.parse::<i32>() {
                        let key = format!("{}:{}", topic, partition);
                        let log = PartitionLog::open(&self.config, &topic, partition)?;
                        self.partitions.write().insert(key, Arc::new(log));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn append(&self, message: &Message) -> Result<i64, StorageError> {
        let key = format!("{}:{}", message.topic, message.partition);
        
        let partition = {
            let partitions = self.partitions.read();
            if let Some(log) = partitions.get(&key) {
                log.clone()
            } else {
                drop(partitions);
                let mut partitions = self.partitions.write();
                let log = Arc::new(PartitionLog::open(&self.config, &message.topic, message.partition)?);
                partitions.insert(key.clone(), log.clone());
                log
            }
        };

        partition.append(message)
    }

    pub fn fetch(&self, topic: &str, partition: i32, offset: i64, max_bytes: u32) -> Result<(Vec<Message>, i64), StorageError> {
        let key = format!("{}:{}", topic, partition);
        
        let partitions = self.partitions.read();
        if let Some(log) = partitions.get(&key) {
            log.fetch(offset, max_bytes)
        } else {
            Err(StorageError::SegmentNotFound)
        }
    }

    pub fn get_leo(&self, topic: &str, partition: i32) -> i64 {
        let key = format!("{}:{}", topic, partition);
        let partitions = self.partitions.read();
        partitions.get(&key).map(|l| l.leo()).unwrap_or(0)
    }
}

pub struct PartitionLog {
    topic: String,
    partition: i32,
    base_dir: PathBuf,
    segment_size: u64,
    segments: RwLock<Vec<Arc<Segment>>>,
    active_segment: Mutex<Arc<Segment>>,
    leo: RwLock<i64>,
    hw: RwLock<i64>,
}

impl PartitionLog {
    pub fn open(config: &BrokerConfig, topic: &str, partition: i32) -> Result<Self, StorageError> {
        let base_dir = Path::new(&config.data_dir)
            .join(topic)
            .join(format!("partition-{}", partition));
        
        fs::create_dir_all(&base_dir)?;

        let mut segments = Vec::new();
        let mut leo = 0i64;

        if base_dir.exists() {
            for entry in fs::read_dir(&base_dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(".log") {
                    if let Some(base_offset_str) = name.strip_suffix(".log") {
                        if let Ok(base_offset) = base_offset_str.parse::<i64>() {
                            let segment = Segment::open(&base_dir, base_offset)?;
                            leo = leo.max(segment.base_offset() + segment.size() as i64);
                            segments.push(Arc::new(segment));
                        }
                    }
                }
            }
        }

        segments.sort_by_key(|s| s.base_offset());

        let active_segment = if segments.is_empty() {
            let seg = Arc::new(Segment::create(&base_dir, 0)?);
            segments.push(seg.clone());
            seg
        } else {
            segments.last().unwrap().clone()
        };

        Ok(Self {
            topic: topic.to_string(),
            partition,
            base_dir,
            segment_size: config.segment_size as u64,
            segments: RwLock::new(segments),
            active_segment: Mutex::new(active_segment),
            leo: RwLock::new(leo),
            hw: RwLock::new(0),
        })
    }

    pub fn append(&self, message: &Message) -> Result<i64, StorageError> {
        let offset = *self.leo.read();
        
        let mut active_segment = self.active_segment.lock();
        
        if active_segment.size() >= self.segment_size {
            let new_segment = Arc::new(Segment::create(&self.base_dir, offset)?);
            self.segments.write().push(new_segment.clone());
            *active_segment = new_segment;
        }

        active_segment.append(message, offset)?;
        *self.leo.write() = offset + 1;

        Ok(offset)
    }

    pub fn fetch(&self, offset: i64, max_bytes: u32) -> Result<(Vec<Message>, i64), StorageError> {
        let leo = *self.leo.read();
        if offset >= leo {
            return Ok((Vec::new(), leo));
        }

        let segments = self.segments.read();
        let segment = segments.iter()
            .find(|s| offset >= s.base_offset() && offset < s.base_offset() + s.size() as i64)
            .cloned()
            .ok_or(StorageError::OffsetOutOfRange)?;

        let messages = segment.read(offset, max_bytes)?;
        Ok((messages, leo))
    }

    pub fn leo(&self) -> i64 {
        *self.leo.read()
    }

    pub fn hw(&self) -> i64 {
        *self.hw.read()
    }
}

pub struct Segment {
    base_offset: i64,
    file: RwLock<File>,
    writer: Mutex<BufWriter<File>>,
    log_path: PathBuf,
    index_path: PathBuf,
    time_index_path: PathBuf,
    index: RwLock<Vec<IndexEntry>>,
    time_index: RwLock<Vec<TimeIndexEntry>>,
    size: RwLock<u64>,
    sealed: RwLock<bool>,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    pub offset: i64,
    pub position: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct TimeIndexEntry {
    pub timestamp: i64,
    pub offset: i64,
}

impl Segment {
    pub fn create(base_dir: &Path, base_offset: i64) -> Result<Self, StorageError> {
        let log_path = base_dir.join(format!("{:020}.log", base_offset));
        let index_path = base_dir.join(format!("{:020}.index", base_offset));
        let time_index_path = base_dir.join(format!("{:020}.timeindex", base_offset));

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&log_path)?;

        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&index_path)?;
        drop(index_file);

        let time_index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&time_index_path)?;
        drop(time_index_file);

        let writer = BufWriter::with_capacity(1024 * 1024, file.try_clone()?);

        Ok(Self {
            base_offset,
            file: RwLock::new(file),
            writer: Mutex::new(writer),
            log_path,
            index_path,
            time_index_path,
            index: RwLock::new(Vec::new()),
            time_index: RwLock::new(Vec::new()),
            size: RwLock::new(0),
            sealed: RwLock::new(false),
        })
    }

    pub fn open(base_dir: &Path, base_offset: i64) -> Result<Self, StorageError> {
        let mut segment = Self::create(base_dir, base_offset)?;
        
        let file = segment.file.read();
        let metadata = file.metadata()?;
        *segment.size.write() = metadata.len();
        
        Ok(segment)
    }

    pub fn append(&self, message: &Message, offset: i64) -> Result<(), StorageError> {
        let data = encode_message(message, offset)?;
        
        let mut writer = self.writer.lock();
        writer.write_all(&(data.len() as u32).to_be_bytes())?;
        writer.write_all(&data)?;
        writer.flush()?;

        let new_size = *self.size.read() + 4 + data.len() as u64;
        *self.size.write() = new_size;

        let relative_offset = offset - self.base_offset;
        if relative_offset % INDEX_INTERVAL as i64 == 0 {
            let entry = IndexEntry {
                offset,
                position: new_size - 4 - data.len() as u64,
            };
            self.index.write().push(entry);
        }

        let time_entry = TimeIndexEntry {
            timestamp: message.timestamp,
            offset,
        };
        self.time_index.write().push(time_entry);

        Ok(())
    }

    pub fn read(&self, offset: i64, max_bytes: u32) -> Result<Vec<Message>, StorageError> {
        let position = self.find_position(offset);
        
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(position))?;

        let mut messages = Vec::new();
        let mut bytes_read = 0u32;

        while bytes_read < max_bytes {
            let mut len_bytes = [0u8; 4];
            match file.read_exact(&mut len_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            
            let len = u32::from_be_bytes(len_bytes);
            if len == 0 || len > 16 * 1024 * 1024 {
                break;
            }

            let mut data = vec![0u8; len as usize];
            file.read_exact(&mut data)?;

            if let Ok(msg) = decode_message(&data) {
                messages.push(msg);
            }

            bytes_read += 4 + len;
        }

        Ok(messages)
    }

    fn find_position(&self, offset: i64) -> u64 {
        let index = self.index.read();
        let idx = index.partition_point(|e| e.offset < offset);
        if idx > 0 {
            index[idx - 1].position
        } else {
            0
        }
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    pub fn size(&self) -> u64 {
        *self.size.read()
    }

    pub fn seal(&self) -> Result<(), StorageError> {
        *self.sealed.write() = true;
        Ok(())
    }
}

fn encode_message(msg: &Message, offset: i64) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(&offset.to_be_bytes());
    buf.extend_from_slice(&(msg.topic.len() as u32).to_be_bytes());
    buf.extend_from_slice(msg.topic.as_bytes());

    if let Some(ref key) = msg.key {
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
    } else {
        buf.extend_from_slice(&0u32.to_be_bytes());
    }

    buf.extend_from_slice(&(msg.value.len() as u32).to_be_bytes());
    buf.extend_from_slice(&msg.value);

    buf.extend_from_slice(&msg.timestamp.to_be_bytes());
    buf.extend_from_slice(&msg.deliver_at.to_be_bytes());

    buf.extend_from_slice(&(msg.headers.len() as u32).to_be_bytes());
    for (k, v) in &msg.headers {
        buf.extend_from_slice(&(k.len() as u16).to_be_bytes());
        buf.extend_from_slice(k.as_bytes());
        buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        buf.extend_from_slice(v.as_bytes());
    }

    Ok(buf)
}

fn decode_message(data: &[u8]) -> Result<Message, StorageError> {
    let mut offset = 0;

    let msg_offset = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
    offset += 8;

    let topic_len = u32::from_be_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
    offset += 4;
    let topic = String::from_utf8_lossy(&data[offset..offset+topic_len]).to_string();
    offset += topic_len;

    let key_len = u32::from_be_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
    offset += 4;
    let key = if key_len > 0 {
        Some(data[offset..offset+key_len].to_vec())
    } else {
        None
    };
    offset += key_len;

    let value_len = u32::from_be_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
    offset += 4;
    let value = data[offset..offset+value_len].to_vec();
    offset += value_len;

    let timestamp = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
    offset += 8;
    let deliver_at = i64::from_be_bytes(data[offset..offset+8].try_into().unwrap());
    offset += 8;

    let header_count = u32::from_be_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
    offset += 4;

    let mut headers = std::collections::HashMap::new();
    for _ in 0..header_count {
        let k_len = u16::from_be_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
        offset += 2;
        let k = String::from_utf8_lossy(&data[offset..offset+k_len]).to_string();
        offset += k_len;

        let v_len = u16::from_be_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
        offset += 2;
        let v = String::from_utf8_lossy(&data[offset..offset+v_len]).to_string();
        offset += v_len;

        headers.insert(k, v);
    }

    Ok(Message {
        message_id: uuid::Uuid::new_v4().to_string(),
        topic,
        partition: 0,
        offset: msg_offset,
        key,
        value,
        headers,
        timestamp,
        partition_key: None,
        ordering_key: None,
        deliver_at,
        ttl: 0,
        max_retry: 16,
        retry_count: 0,
        trace_id: None,
        producer_id: None,
        sequence_number: 0,
        transaction_id: None,
    })
}
