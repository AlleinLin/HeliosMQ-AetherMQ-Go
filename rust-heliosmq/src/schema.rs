use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Avro => write!(f, "AVRO"),
            SchemaType::Protobuf => write!(f, "PROTOBUF"),
            SchemaType::Json => write!(f, "JSON"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityMode {
    None,
    Backward,
    Forward,
    Full,
    BackwardTransitive,
    ForwardTransitive,
    FullTransitive,
}

impl Default for CompatibilityMode {
    fn default() -> Self {
        CompatibilityMode::Backward
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub id: i64,
    pub subject: String,
    pub version: i32,
    pub schema: String,
    pub schema_type: SchemaType,
    pub references: Vec<Reference>,
    pub metadata: HashMap<String, String>,
    pub created_at: i64,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subject {
    pub name: String,
    pub compatibility: CompatibilityMode,
    pub version_count: i32,
    pub latest_version: i32,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct SchemaConfig {
    pub enabled: bool,
    pub default_compatibility: CompatibilityMode,
    pub max_schemas_per_subject: usize,
    pub retention_days: i32,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_compatibility: CompatibilityMode::Backward,
            max_schemas_per_subject: 1000,
            retention_days: 30,
        }
    }
}

pub struct SchemaRegistry {
    config: SchemaConfig,
    schemas: RwLock<HashMap<String, Schema>>,
    subjects: RwLock<HashMap<String, Subject>>,
    id_counter: RwLock<i64>,
    validators: HashMap<SchemaType, Box<dyn SchemaValidator + Send + Sync>>,
}

impl SchemaRegistry {
    pub fn new(config: SchemaConfig) -> Self {
        let mut validators: HashMap<SchemaType, Box<dyn SchemaValidator + Send + Sync>> = HashMap::new();
        validators.insert(SchemaType::Avro, Box::new(AvroValidator));
        validators.insert(SchemaType::Protobuf, Box::new(ProtobufValidator));
        validators.insert(SchemaType::Json, Box::new(JsonValidator));

        Self {
            config,
            schemas: RwLock::new(HashMap::new()),
            subjects: RwLock::new(HashMap::new()),
            id_counter: RwLock::new(0),
            validators,
        }
    }

    pub fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
    ) -> anyhow::Result<Schema> {
        if !self.config.enabled {
            return Err(anyhow::anyhow!("Schema registry disabled"));
        }

        let validator = self.validators.get(&schema_type)
            .ok_or_else(|| anyhow::anyhow!("Unsupported schema type: {}", schema_type))?;

        validator.validate(schema)?;

        let canonical_schema = validator.canonicalize(schema)?;
        let fingerprint = validator.fingerprint(&canonical_schema)?;

        let mut schemas = self.schemas.write();
        let mut subjects = self.subjects.write();

        let subject_info = subjects.entry(subject.to_string()).or_insert_with(|| {
            Subject {
                name: subject.to_string(),
                compatibility: self.config.default_compatibility,
                version_count: 0,
                latest_version: 0,
                created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                updated_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            }
        });

        if subject_info.version_count >= self.config.max_schemas_per_subject as i32 {
            return Err(anyhow::anyhow!("Max schemas per subject reached"));
        }

        let latest_key = format!("{}:v{}", subject, subject_info.latest_version);
        if let Some(latest_schema) = schemas.get(&latest_key) {
            if !latest_schema.deleted {
                validator.validate_compatibility(
                    &latest_schema.schema,
                    &canonical_schema,
                    subject_info.compatibility,
                )?;
            }
        }

        let existing = self.find_by_fingerprint(&schemas, subject, &fingerprint);
        if let Some(existing) = existing {
            return Ok(existing.clone());
        }

        let mut id_counter = self.id_counter.write();
        *id_counter += 1;

        let new_schema = Schema {
            id: *id_counter,
            subject: subject.to_string(),
            version: subject_info.latest_version + 1,
            schema: canonical_schema,
            schema_type,
            references: Vec::new(),
            metadata: HashMap::new(),
            created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            deleted: false,
        };

        let key = format!("{}:v{}", subject, new_schema.version);
        schemas.insert(key, new_schema.clone());

        subject_info.version_count += 1;
        subject_info.latest_version = new_schema.version;
        subject_info.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

        Ok(new_schema)
    }

    pub fn get_schema_by_id(&self, id: i64) -> Option<Schema> {
        self.schemas.read().values().find(|s| s.id == id).cloned()
    }

    pub fn get_schema_by_subject(&self, subject: &str, version: i32) -> Option<Schema> {
        let key = format!("{}:v{}", subject, version);
        self.schemas.read().get(&key).cloned()
    }

    pub fn get_latest_schema(&self, subject: &str) -> Option<Schema> {
        let subjects = self.subjects.read();
        let subject_info = subjects.get(subject)?;
        let key = format!("{}:v{}", subject, subject_info.latest_version);
        self.schemas.read().get(&key).cloned()
    }

    pub fn list_subjects(&self) -> Vec<String> {
        let mut subjects: Vec<_> = self.subjects.read().keys().cloned().collect();
        subjects.sort();
        subjects
    }

    pub fn list_versions(&self, subject: &str) -> Vec<i32> {
        let schemas = self.schemas.read();
        let mut versions: Vec<i32> = schemas
            .iter()
            .filter(|(_, s)| s.subject == subject && !s.deleted)
            .map(|(_, s)| s.version)
            .collect();
        versions.sort();
        versions
    }

    pub fn delete_subject(&self, subject: &str) -> anyhow::Result<()> {
        let mut schemas = self.schemas.write();
        for (_, schema) in schemas.iter_mut() {
            if schema.subject == subject {
                schema.deleted = true;
            }
        }

        self.subjects.write().remove(subject);
        Ok(())
    }

    pub fn delete_schema_version(&self, subject: &str, version: i32) -> anyhow::Result<()> {
        let key = format!("{}:v{}", subject, version);
        if let Some(schema) = self.schemas.write().get_mut(&key) {
            schema.deleted = true;
        }
        Ok(())
    }

    pub fn set_compatibility(&self, subject: &str, mode: CompatibilityMode) -> anyhow::Result<()> {
        let mut subjects = self.subjects.write();
        let subject_info = subjects.entry(subject.to_string()).or_insert_with(|| {
            Subject {
                name: subject.to_string(),
                compatibility: self.config.default_compatibility,
                version_count: 0,
                latest_version: 0,
                created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                updated_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            }
        });
        subject_info.compatibility = mode;
        Ok(())
    }

    pub fn get_compatibility(&self, subject: &str) -> CompatibilityMode {
        if subject.is_empty() {
            return self.config.default_compatibility;
        }
        self.subjects.read()
            .get(subject)
            .map(|s| s.compatibility)
            .unwrap_or(self.config.default_compatibility)
    }

    fn find_by_fingerprint(&self, schemas: &HashMap<String, Schema>, subject: &str, fingerprint: &str) -> Option<&Schema> {
        let validator = self.validators.get(&SchemaType::Avro)?;
        schemas.values().find(|s| {
            if s.subject != subject || s.deleted {
                return false;
            }
            if let Some(v) = self.validators.get(&s.schema_type) {
                if let Ok(fp) = v.fingerprint(&s.schema) {
                    return fp == fingerprint;
                }
            }
            false
        })
    }

    pub fn validate_message(&self, subject: &str, version: i32, message: &[u8]) -> anyhow::Result<()> {
        let schema = self.get_schema_by_subject(subject, version)
            .ok_or_else(|| anyhow::anyhow!("Schema not found"))?;

        let validator = self.validators.get(&schema.schema_type)
            .ok_or_else(|| anyhow::anyhow!("Unsupported schema type"))?;

        validator.validate(&String::from_utf8_lossy(message))
    }
}

trait SchemaValidator: Send + Sync {
    fn validate(&self, schema: &str) -> anyhow::Result<()>;
    fn validate_compatibility(&self, old_schema: &str, new_schema: &str, mode: CompatibilityMode) -> anyhow::Result<()>;
    fn canonicalize(&self, schema: &str) -> anyhow::Result<String>;
    fn fingerprint(&self, schema: &str) -> anyhow::Result<String>;
}

struct AvroValidator;

impl SchemaValidator for AvroValidator {
    fn validate(&self, schema: &str) -> anyhow::Result<()> {
        let parsed: serde_json::Value = serde_json::from_str(schema)?;
        if !parsed.is_object() {
            return Err(anyhow::anyhow!("Invalid Avro schema"));
        }
        Ok(())
    }

    fn validate_compatibility(&self, old_schema: &str, new_schema: &str, mode: CompatibilityMode) -> anyhow::Result<()> {
        match mode {
            CompatibilityMode::None => Ok(()),
            CompatibilityMode::Backward | CompatibilityMode::BackwardTransitive => {
                self.check_backward_compatibility(old_schema, new_schema)
            }
            CompatibilityMode::Forward | CompatibilityMode::ForwardTransitive => {
                self.check_forward_compatibility(old_schema, new_schema)
            }
            CompatibilityMode::Full | CompatibilityMode::FullTransitive => {
                self.check_backward_compatibility(old_schema, new_schema)?;
                self.check_forward_compatibility(old_schema, new_schema)
            }
        }
    }

    fn canonicalize(&self, schema: &str) -> anyhow::Result<String> {
        let parsed: serde_json::Value = serde_json::from_str(schema)?;
        Ok(serde_json::to_string(&parsed)?)
    }

    fn fingerprint(&self, schema: &str) -> anyhow::Result<String> {
        let canonical = self.canonicalize(schema)?;
        Ok(format!("avro-{:x}", hash_string(&canonical)))
    }
}

impl AvroValidator {
    fn check_backward_compatibility(&self, _old_schema: &str, _new_schema: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn check_forward_compatibility(&self, _old_schema: &str, _new_schema: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

struct ProtobufValidator;

impl SchemaValidator for ProtobufValidator {
    fn validate(&self, schema: &str) -> anyhow::Result<()> {
        if schema.trim().is_empty() {
            return Err(anyhow::anyhow!("Empty protobuf schema"));
        }
        Ok(())
    }

    fn validate_compatibility(&self, _old_schema: &str, _new_schema: &str, _mode: CompatibilityMode) -> anyhow::Result<()> {
        Ok(())
    }

    fn canonicalize(&self, schema: &str) -> anyhow::Result<String> {
        Ok(schema.trim().to_string())
    }

    fn fingerprint(&self, schema: &str) -> anyhow::Result<String> {
        let canonical = self.canonicalize(schema)?;
        Ok(format!("proto-{:x}", hash_string(&canonical)))
    }
}

struct JsonValidator;

impl SchemaValidator for JsonValidator {
    fn validate(&self, schema: &str) -> anyhow::Result<()> {
        let _: serde_json::Value = serde_json::from_str(schema)?;
        Ok(())
    }

    fn validate_compatibility(&self, _old_schema: &str, _new_schema: &str, _mode: CompatibilityMode) -> anyhow::Result<()> {
        Ok(())
    }

    fn canonicalize(&self, schema: &str) -> anyhow::Result<String> {
        let parsed: serde_json::Value = serde_json::from_str(schema)?;
        Ok(serde_json::to_string(&parsed)?)
    }

    fn fingerprint(&self, schema: &str) -> anyhow::Result<String> {
        let canonical = self.canonicalize(schema)?;
        Ok(format!("json-{:x}", hash_string(&canonical)))
    }
}

fn hash_string(s: &str) -> u32 {
    let mut hash: u32 = 0;
    for c in s.chars() {
        hash = hash.wrapping_mul(31).wrapping_add(c as u32);
    }
    hash
}
