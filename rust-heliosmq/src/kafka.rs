use std::collections::HashMap;
use std::io::{self, Read, Write, Cursor};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use parking_lot::RwLock;
use tracing::{info, warn, error};

use crate::broker::Broker;
use crate::common::Message;

const API_KEY_PRODUCE: i16 = 0;
const API_KEY_FETCH: i16 = 1;
const API_KEY_LIST_OFFSETS: i16 = 2;
const API_KEY_METADATA: i16 = 3;
const API_KEY_OFFSET_COMMIT: i16 = 8;
const API_KEY_OFFSET_FETCH: i16 = 9;
const API_KEY_FIND_COORDINATOR: i16 = 10;
const API_KEY_JOIN_GROUP: i16 = 11;
const API_KEY_HEARTBEAT: i16 = 12;
const API_KEY_LEAVE_GROUP: i16 = 13;
const API_KEY_SYNC_GROUP: i16 = 14;
const API_KEY_API_VERSIONS: i16 = 18;
const API_KEY_CREATE_TOPICS: i16 = 19;
const API_KEY_DELETE_TOPICS: i16 = 20;
const API_KEY_INIT_PRODUCER_ID: i16 = 22;
const API_KEY_END_TXN: i16 = 26;

#[derive(Debug, Clone)]
pub struct KafkaProtocolConfig {
    pub listen_addr: String,
    pub max_connections: usize,
    pub max_request_size: i32,
    pub max_response_size: i32,
    pub request_timeout: Duration,
    pub socket_send_buffer: usize,
    pub socket_recv_buffer: usize,
}

impl Default for KafkaProtocolConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9092".to_string(),
            max_connections: 10000,
            max_request_size: 100 * 1024 * 1024,
            max_response_size: 100 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            socket_send_buffer: 256 * 1024,
            socket_recv_buffer: 256 * 1024,
        }
    }
}

pub struct KafkaProtocolServer {
    config: KafkaProtocolConfig,
    broker: Arc<Broker>,
    listener: Option<TcpListener>,
    connections: RwLock<HashMap<String, TcpStream>>,
    running: RwLock<bool>,
    api_versions: HashMap<i16, i16>,
}

impl KafkaProtocolServer {
    pub fn new(config: KafkaProtocolConfig, broker: Arc<Broker>) -> Self {
        let mut api_versions = HashMap::new();
        api_versions.insert(API_KEY_PRODUCE, 9);
        api_versions.insert(API_KEY_FETCH, 12);
        api_versions.insert(API_KEY_LIST_OFFSETS, 6);
        api_versions.insert(API_KEY_METADATA, 11);
        api_versions.insert(API_KEY_OFFSET_COMMIT, 8);
        api_versions.insert(API_KEY_OFFSET_FETCH, 8);
        api_versions.insert(API_KEY_FIND_COORDINATOR, 4);
        api_versions.insert(API_KEY_JOIN_GROUP, 7);
        api_versions.insert(API_KEY_HEARTBEAT, 4);
        api_versions.insert(API_KEY_LEAVE_GROUP, 4);
        api_versions.insert(API_KEY_SYNC_GROUP, 5);
        api_versions.insert(API_KEY_API_VERSIONS, 3);
        api_versions.insert(API_KEY_CREATE_TOPICS, 7);
        api_versions.insert(API_KEY_DELETE_TOPICS, 6);
        api_versions.insert(API_KEY_INIT_PRODUCER_ID, 5);
        api_versions.insert(API_KEY_END_TXN, 3);

        Self {
            config,
            broker,
            listener: None,
            connections: RwLock::new(HashMap::new()),
            running: RwLock::new(false),
            api_versions,
        }
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr)?;
        self.listener = Some(listener);
        *self.running.write() = true;

        let listener = self.listener.clone();
        let running = self.running.clone();
        let config = self.config.clone();
        let broker = self.broker.clone();
        let api_versions = self.api_versions.clone();

        std::thread::spawn(move || {
            while *running.read() {
                if let Some(ref lst) = listener {
                    match lst.accept() {
                        Ok((stream, addr)) => {
                            let addr_str = addr.to_string();
                            if config.max_connections > 0 {
                                let conn_count = 0;
                                if conn_count >= config.max_connections {
                                    let _ = stream.shutdown(std::net::Shutdown::Both);
                                    continue;
                                }
                            }

                            let broker = broker.clone();
                            let api_versions = api_versions.clone();
                            let config = config.clone();

                            std::thread::spawn(move || {
                                handle_connection(stream, &config, &broker, &api_versions);
                            });
                        }
                        Err(e) => {
                            warn!("Failed to accept connection: {}", e);
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub fn stop(&self) {
        *self.running.write() = false;
        if let Some(ref listener) = self.listener {
            let _ = listener;
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    config: &KafkaProtocolConfig,
    broker: &Arc<Broker>,
    api_versions: &HashMap<i16, i16>,
) {
    let _ = stream.set_read_timeout(Some(config.request_timeout));
    let _ = stream.set_write_timeout(Some(config.request_timeout));

    loop {
        let mut size_buf = [0u8; 4];
        match stream.read_exact(&mut size_buf) {
            Ok(_) => {}
            Err(_) => break,
        }

        let size = i32::from_be_bytes(size_buf);
        if size > config.max_request_size || size < 0 {
            break;
        }

        let mut request_buf = vec![0u8; size as usize];
        if stream.read_exact(&mut request_buf).is_err() {
            break;
        }

        let response = process_request(&request_buf, broker, api_versions);

        let response_len = response.len() as i32;
        if stream.write_all(&response_len.to_be_bytes()).is_err() {
            break;
        }
        if stream.write_all(&response).is_err() {
            break;
        }
    }
}

fn process_request(data: &[u8], broker: &Arc<Broker>, api_versions: &HashMap<i16, i16>) -> Vec<u8> {
    let mut reader = RequestReader::new(data);

    let api_key = match reader.read_i16() {
        Ok(v) => v,
        Err(_) => return error_response(0, 0),
    };

    let api_version = match reader.read_i16() {
        Ok(v) => v,
        Err(_) => return error_response(0, 0),
    };

    let correlation_id = match reader.read_i32() {
        Ok(v) => v,
        Err(_) => return error_response(0, 0),
    };

    let _client_id: String = reader.read_string().unwrap_or_default();

    let max_version = api_versions.get(&api_key).copied().unwrap_or(0);

    if api_version > max_version {
        return error_response(correlation_id, 35);
    }

    match api_key {
        API_KEY_API_VERSIONS => handle_api_versions(correlation_id, api_versions),
        API_KEY_METADATA => handle_metadata(correlation_id, api_version, &mut reader, broker),
        API_KEY_PRODUCE => handle_produce(correlation_id, api_version, &mut reader, broker),
        API_KEY_FETCH => handle_fetch(correlation_id, api_version, &mut reader, broker),
        API_KEY_OFFSET_COMMIT => handle_offset_commit(correlation_id, api_version, &mut reader, broker),
        API_KEY_OFFSET_FETCH => handle_offset_fetch(correlation_id, api_version, &mut reader, broker),
        API_KEY_FIND_COORDINATOR => handle_find_coordinator(correlation_id),
        API_KEY_JOIN_GROUP => handle_join_group(correlation_id, api_version, &mut reader),
        API_KEY_HEARTBEAT => handle_heartbeat(correlation_id, api_version, &mut reader),
        API_KEY_LEAVE_GROUP => handle_leave_group(correlation_id, api_version, &mut reader),
        API_KEY_SYNC_GROUP => handle_sync_group(correlation_id),
        API_KEY_CREATE_TOPICS => handle_create_topics(correlation_id, api_version, &mut reader),
        API_KEY_DELETE_TOPICS => handle_delete_topics(correlation_id, api_version, &mut reader),
        API_KEY_INIT_PRODUCER_ID => handle_init_producer_id(correlation_id),
        API_KEY_END_TXN => handle_end_txn(correlation_id),
        _ => error_response(correlation_id, 35),
    }
}

fn handle_api_versions(correlation_id: i32, api_versions: &HashMap<i16, i16>) -> Vec<u8> {
    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);

    writer.write_i32(api_versions.len() as i32);
    for (&key, &version) in api_versions {
        writer.write_i16(key);
        writer.write_i16(0);
        writer.write_i16(version);
    }

    writer.bytes()
}

fn handle_metadata(correlation_id: i32, api_version: i16, reader: &mut RequestReader, broker: &Arc<Broker>) -> Vec<u8> {
    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);

    let topic_count = reader.read_i32().unwrap_or(0);
    let mut topics = Vec::new();
    for _ in 0..topic_count {
        if let Ok(topic) = reader.read_string() {
            topics.push(topic);
        }
    }

    writer.write_i32(1);
    writer.write_i32(0);
    writer.write_string("localhost");
    writer.write_i32(9092);

    if api_version >= 2 {
        writer.write_i32(-1);
    }

    writer.write_i32(topics.len() as i32);
    for topic in &topics {
        writer.write_i16(0);
        writer.write_string(topic);
        writer.write_i16(0);
        writer.write_i32(1);
        writer.write_i32(1);

        writer.write_i16(0);
        writer.write_i32(0);
        writer.write_i32(0);
        writer.write_i32(1);
        writer.write_i32(0);
        writer.write_i32(1);
        writer.write_i32(0);

        if api_version >= 5 {
            writer.write_i32(-1);
        }
    }

    writer.bytes()
}

fn handle_produce(correlation_id: i32, api_version: i16, reader: &mut RequestReader, broker: &Arc<Broker>) -> Vec<u8> {
    let _acks = reader.read_i16().unwrap_or(1);
    let _timeout = reader.read_i32().unwrap_or(30000);

    if api_version >= 1 {
        let _ = reader.read_i32();
    }

    let topic_count = reader.read_i32().unwrap_or(0);

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);

    if api_version >= 1 {
        writer.write_i32(0);
    }

    writer.write_i32(topic_count);

    for _ in 0..topic_count {
        let topic = reader.read_string().unwrap_or_default();
        let partition_count = reader.read_i32().unwrap_or(0);

        writer.write_string(&topic);
        writer.write_i32(partition_count);

        for _ in 0..partition_count {
            let _partition = reader.read_i32().unwrap_or(0);
            let record_set_size = reader.read_i32().unwrap_or(0);

            if record_set_size > 0 {
                let mut record_data = vec![0u8; record_set_size as usize];
                let _ = reader.read(&mut record_data);
            }

            writer.write_i16(0);
            writer.write_i64(0);

            if api_version >= 2 {
                writer.write_i64(-1);
            }
            if api_version >= 5 {
                writer.write_i64(-1);
            }
        }
    }

    if api_version >= 1 {
        writer.write_i32(0);
    }

    writer.bytes()
}

fn handle_fetch(correlation_id: i32, api_version: i16, reader: &mut RequestReader, broker: &Arc<Broker>) -> Vec<u8> {
    let _max_wait = reader.read_i32().unwrap_or(500);
    let _min_bytes = reader.read_i32().unwrap_or(1);

    if api_version >= 3 {
        let _ = reader.read_i32();
    }
    if api_version >= 4 {
        let _ = reader.read_i8();
    }
    if api_version >= 7 {
        let _ = reader.read_i32();
        let _ = reader.read_i64();
    }

    let topic_count = reader.read_i32().unwrap_or(0);

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);

    if api_version >= 1 {
        writer.write_i32(0);
    }

    writer.write_i32(topic_count);

    for _ in 0..topic_count {
        let topic = reader.read_string().unwrap_or_default();
        let partition_count = reader.read_i32().unwrap_or(0);

        writer.write_string(&topic);
        writer.write_i32(partition_count);

        for _ in 0..partition_count {
            let _partition = reader.read_i32().unwrap_or(0);
            let _fetch_offset = reader.read_i64().unwrap_or(0);
            let _partition_max_bytes = reader.read_i32().unwrap_or(1024 * 1024);

            if api_version >= 5 {
                let _ = reader.read_i64();
            }

            writer.write_i16(0);
            writer.write_i64(0);

            if api_version >= 4 {
                writer.write_i64(0);
            }
            if api_version >= 5 {
                writer.write_i64(0);
            }

            writer.write_i32(0);
        }
    }

    writer.bytes()
}

fn handle_offset_commit(correlation_id: i32, api_version: i16, reader: &mut RequestReader, broker: &Arc<Broker>) -> Vec<u8> {
    let _group_id = reader.read_string().unwrap_or_default();

    if api_version >= 1 {
        let _ = reader.read_i32();
        let _ = reader.read_string();
        let _ = reader.read_i64();
    }

    let topic_count = reader.read_i32().unwrap_or(0);

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i32(topic_count);

    for _ in 0..topic_count {
        let _topic = reader.read_string().unwrap_or_default();
        let partition_count = reader.read_i32().unwrap_or(0);

        writer.write_string("");
        writer.write_i32(partition_count);

        for _ in 0..partition_count {
            let _partition = reader.read_i32().unwrap_or(0);
            let _offset = reader.read_i64().unwrap_or(0);
            let _ = reader.read_string();
            if api_version >= 2 {
                let _ = reader.read_string();
            }

            writer.write_i16(0);
        }
    }

    writer.bytes()
}

fn handle_offset_fetch(correlation_id: i32, api_version: i16, reader: &mut RequestReader, broker: &Arc<Broker>) -> Vec<u8> {
    let _group_id = reader.read_string().unwrap_or_default();

    if api_version >= 1 {
        let _ = reader.read_string();
    }

    let topic_count = reader.read_i32().unwrap_or(0);

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i32(topic_count);

    for _ in 0..topic_count {
        let _topic = reader.read_string().unwrap_or_default();
        let partition_count = reader.read_i32().unwrap_or(0);

        writer.write_string("");
        writer.write_i32(partition_count);

        for _ in 0..partition_count {
            let _partition = reader.read_i32().unwrap_or(0);

            writer.write_i64(0);
            writer.write_string("");
            writer.write_i16(0);
        }
    }

    if api_version >= 3 {
        writer.write_i16(0);
    }

    writer.bytes()
}

fn handle_find_coordinator(correlation_id: i32) -> Vec<u8> {
    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i16(0);
    writer.write_i32(0);
    writer.write_string("localhost");
    writer.write_i32(9092);
    writer.bytes()
}

fn handle_join_group(correlation_id: i32, api_version: i16, reader: &mut RequestReader) -> Vec<u8> {
    let _group_id = reader.read_string().unwrap_or_default();
    let _session_timeout = reader.read_i32().unwrap_or(10000);
    if api_version >= 1 {
        let _ = reader.read_i32();
    }
    let _ = reader.read_string();
    let _ = reader.read_string();
    let _ = reader.read_string();
    let _ = reader.read_i32();

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i16(0);
    writer.write_i32(1);
    writer.write_string("member-1");
    writer.write_string("member-1");
    writer.write_i32(0);
    writer.bytes()
}

fn handle_heartbeat(correlation_id: i32, api_version: i16, reader: &mut RequestReader) -> Vec<u8> {
    let _group_id = reader.read_string().unwrap_or_default();
    let _ = reader.read_i32();
    let _ = reader.read_string();
    let _ = reader.read_string();

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.bytes()
}

fn handle_leave_group(correlation_id: i32, api_version: i16, reader: &mut RequestReader) -> Vec<u8> {
    let _group_id = reader.read_string().unwrap_or_default();
    let _ = reader.read_string();

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.bytes()
}

fn handle_sync_group(correlation_id: i32) -> Vec<u8> {
    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i32(0);
    writer.bytes()
}

fn handle_create_topics(correlation_id: i32, api_version: i16, reader: &mut RequestReader) -> Vec<u8> {
    let topic_count = reader.read_i32().unwrap_or(0);

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i32(topic_count);

    for _ in 0..topic_count {
        let _topic = reader.read_string().unwrap_or_default();
        let _partitions = reader.read_i32().unwrap_or(1);
        let _replication = reader.read_i16().unwrap_or(1);

        writer.write_i16(0);
        writer.write_string("");
    }

    if api_version >= 1 {
        writer.write_i32(0);
    }

    writer.bytes()
}

fn handle_delete_topics(correlation_id: i32, api_version: i16, reader: &mut RequestReader) -> Vec<u8> {
    let topic_count = reader.read_i32().unwrap_or(0);

    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i32(topic_count);

    for _ in 0..topic_count {
        let _topic = reader.read_string().unwrap_or_default();
        writer.write_i16(0);
    }

    writer.bytes()
}

fn handle_init_producer_id(correlation_id: i32) -> Vec<u8> {
    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.write_i64(1);
    writer.write_i16(0);
    writer.bytes()
}

fn handle_end_txn(correlation_id: i32) -> Vec<u8> {
    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(0);
    writer.bytes()
}

fn error_response(correlation_id: i32, error_code: i16) -> Vec<u8> {
    let mut writer = ResponseWriter::new();
    writer.write_i32(correlation_id);
    writer.write_i16(error_code);
    writer.bytes()
}

struct RequestReader<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> RequestReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(data),
        }
    }

    fn read_i8(&mut self) -> io::Result<i8> {
        self.cursor.read_i8()
    }

    fn read_i16(&mut self) -> io::Result<i16> {
        self.cursor.read_i16::<BigEndian>()
    }

    fn read_i32(&mut self) -> io::Result<i32> {
        self.cursor.read_i32::<BigEndian>()
    }

    fn read_i64(&mut self) -> io::Result<i64> {
        self.cursor.read_i64::<BigEndian>()
    }

    fn read_string(&mut self) -> io::Result<String> {
        let len = self.read_i16()?;
        if len < 0 {
            return Ok(String::new());
        }
        let mut buf = vec![0u8; len as usize];
        self.cursor.read_exact(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf).to_string())
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }
}

struct ResponseWriter {
    buffer: Vec<u8>,
}

impl ResponseWriter {
    fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
        }
    }

    fn write_i8(&mut self, v: i8) {
        self.buffer.push(v as u8);
    }

    fn write_i16(&mut self, v: i16) {
        self.buffer.write_i16::<BigEndian>(v).unwrap();
    }

    fn write_i32(&mut self, v: i32) {
        self.buffer.write_i32::<BigEndian>(v).unwrap();
    }

    fn write_i64(&mut self, v: i64) {
        self.buffer.write_i64::<BigEndian>(v).unwrap();
    }

    fn write_string(&mut self, v: &str) {
        if v.is_empty() {
            self.write_i16(-1);
            return;
        }
        self.write_i16(v.len() as i16);
        self.buffer.extend_from_slice(v.as_bytes());
    }

    fn bytes(self) -> Vec<u8> {
        self.buffer
    }
}
