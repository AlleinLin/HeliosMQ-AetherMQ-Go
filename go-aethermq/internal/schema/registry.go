package schema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aethermq/aethermq/internal/metadata"
)

var (
	ErrSchemaNotFound      = errors.New("schema not found")
	ErrSchemaAlreadyExists = errors.New("schema already exists")
	ErrInvalidSchema       = errors.New("invalid schema")
	ErrIncompatibleSchema  = errors.New("incompatible schema")
	ErrSubjectNotFound     = errors.New("subject not found")
)

type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
	SchemaTypeJSON     SchemaType = "JSON"
)

type CompatibilityMode string

const (
	CompatibilityNone        CompatibilityMode = "NONE"
	CompatibilityBackward    CompatibilityMode = "BACKWARD"
	CompatibilityForward     CompatibilityMode = "FORWARD"
	CompatibilityFull        CompatibilityMode = "FULL"
	CompatibilityBackwardTransitive CompatibilityMode = "BACKWARD_TRANSITIVE"
	CompatibilityForwardTransitive  CompatibilityMode = "FORWARD_TRANSITIVE"
	CompatibilityFullTransitive     CompatibilityMode = "FULL_TRANSITIVE"
)

type SchemaConfig struct {
	Enabled            bool
	DefaultCompatibility CompatibilityMode
	MaxSchemasPerSubject int
	RetentionDays       int
}

func DefaultSchemaConfig() *SchemaConfig {
	return &SchemaConfig{
		Enabled:             true,
		DefaultCompatibility: CompatibilityBackward,
		MaxSchemasPerSubject: 1000,
		RetentionDays:        30,
	}
}

type Schema struct {
	ID         int64      `json:"id"`
	Subject    string     `json:"subject"`
	Version    int        `json:"version"`
	Schema     string     `json:"schema"`
	SchemaType SchemaType `json:"schema_type"`
	References []Reference `json:"references,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  int64      `json:"created_at"`
	Deleted    bool       `json:"deleted"`
}

type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type Subject struct {
	Name         string            `json:"name"`
	Compatibility CompatibilityMode `json:"compatibility"`
	VersionCount int               `json:"version_count"`
	LatestVersion int              `json:"latest_version"`
	CreatedAt    int64             `json:"created_at"`
	UpdatedAt    int64             `json:"updated_at"`
}

type SchemaRegistry struct {
	config     *SchemaConfig
	store      *metadata.BoltStore
	schemas    sync.Map
	subjects   sync.Map
	idCounter  int64
	mu         sync.RWMutex
	validators map[SchemaType]SchemaValidator
}

type SchemaValidator interface {
	Validate(schema string) error
	ValidateCompatibility(oldSchema, newSchema string, mode CompatibilityMode) error
	Canonicalize(schema string) (string, error)
	GetFingerprint(schema string) (string, error)
}

func NewSchemaRegistry(config *SchemaConfig, store *metadata.BoltStore) (*SchemaRegistry, error) {
	sr := &SchemaRegistry{
		config: config,
		store:  store,
		validators: map[SchemaType]SchemaValidator{
			SchemaTypeAvro:     NewAvroValidator(),
			SchemaTypeProtobuf: NewProtobufValidator(),
			SchemaTypeJSON:     NewJSONValidator(),
		},
	}

	if err := sr.loadFromStore(); err != nil {
		return nil, fmt.Errorf("failed to load schemas from store: %w", err)
	}

	return sr, nil
}

func (sr *SchemaRegistry) loadFromStore() error {
	return nil
}

func (sr *SchemaRegistry) RegisterSchema(ctx context.Context, subject string, schema string, schemaType SchemaType) (*Schema, error) {
	if !sr.config.Enabled {
		return nil, errors.New("schema registry disabled")
	}

	validator, ok := sr.validators[schemaType]
	if !ok {
		return nil, fmt.Errorf("unsupported schema type: %s", schemaType)
	}

	if err := validator.Validate(schema); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}

	canonicalSchema, err := validator.Canonicalize(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to canonicalize schema: %w", err)
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	subjectInfo := sr.getOrCreateSubject(subject)

	if subjectInfo.VersionCount >= sr.config.MaxSchemasPerSubject {
		return nil, fmt.Errorf("max schemas per subject reached (%d)", sr.config.MaxSchemasPerSubject)
	}

	if latestSchema := sr.getLatestSchema(subject); latestSchema != nil {
		if err := validator.ValidateCompatibility(latestSchema.Schema, canonicalSchema, subjectInfo.Compatibility); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrIncompatibleSchema, err)
		}
	}

	fingerprint, err := validator.GetFingerprint(canonicalSchema)
	if err != nil {
		return nil, err
	}

	if existing := sr.findByFingerprint(subject, fingerprint); existing != nil {
		return existing, nil
	}

	sr.idCounter++
	newSchema := &Schema{
		ID:         sr.idCounter,
		Subject:    subject,
		Version:    subjectInfo.LatestVersion + 1,
		Schema:     canonicalSchema,
		SchemaType: schemaType,
		CreatedAt:  time.Now().UnixMilli(),
	}

	key := schemaKey(subject, newSchema.Version)
	sr.schemas.Store(key, newSchema)

	subjectInfo.VersionCount++
	subjectInfo.LatestVersion = newSchema.Version
	subjectInfo.UpdatedAt = time.Now().UnixMilli()
	sr.subjects.Store(subject, subjectInfo)

	return newSchema, nil
}

func (sr *SchemaRegistry) GetSchemaByID(ctx context.Context, id int64) (*Schema, error) {
	var found *Schema
	sr.schemas.Range(func(key, value interface{}) bool {
		if s, ok := value.(*Schema); ok && s.ID == id {
			found = s
			return false
		}
		return true
	})

	if found == nil {
		return nil, ErrSchemaNotFound
	}
	return found, nil
}

func (sr *SchemaRegistry) GetSchemaBySubject(ctx context.Context, subject string, version int) (*Schema, error) {
	key := schemaKey(subject, version)
	if s, ok := sr.schemas.Load(key); ok {
		return s.(*Schema), nil
	}
	return nil, ErrSchemaNotFound
}

func (sr *SchemaRegistry) GetLatestSchema(ctx context.Context, subject string) (*Schema, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	schema := sr.getLatestSchema(subject)
	if schema == nil {
		return nil, ErrSubjectNotFound
	}
	return schema, nil
}

func (sr *SchemaRegistry) getLatestSchema(subject string) *Schema {
	subjectInfo, ok := sr.subjects.Load(subject)
	if !ok {
		return nil
	}
	s := subjectInfo.(*Subject)
	if s.LatestVersion == 0 {
		return nil
	}
	key := schemaKey(subject, s.LatestVersion)
	if schema, ok := sr.schemas.Load(key); ok {
		return schema.(*Schema)
	}
	return nil
}

func (sr *SchemaRegistry) ListSubjects(ctx context.Context) ([]string, error) {
	var subjects []string
	sr.subjects.Range(func(key, value interface{}) bool {
		subjects = append(subjects, key.(string))
		return true
	})
	sort.Strings(subjects)
	return subjects, nil
}

func (sr *SchemaRegistry) ListVersions(ctx context.Context, subject string) ([]int, error) {
	var versions []int
	sr.schemas.Range(func(key, value interface{}) bool {
		if s, ok := value.(*Schema); ok && s.Subject == subject && !s.Deleted {
			versions = append(versions, s.Version)
		}
		return true
	})
	sort.Ints(versions)
	return versions, nil
}

func (sr *SchemaRegistry) DeleteSubject(ctx context.Context, subject string) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.schemas.Range(func(key, value interface{}) bool {
		if s, ok := value.(*Schema); ok && s.Subject == subject {
			s.Deleted = true
		}
		return true
	})

	sr.subjects.Delete(subject)
	return nil
}

func (sr *SchemaRegistry) DeleteSchemaVersion(ctx context.Context, subject string, version int) error {
	key := schemaKey(subject, version)
	if s, ok := sr.schemas.Load(key); ok {
		schema := s.(*Schema)
		schema.Deleted = true
	}
	return nil
}

func (sr *SchemaRegistry) SetCompatibility(ctx context.Context, subject string, mode CompatibilityMode) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	subjectInfo := sr.getOrCreateSubject(subject)
	subjectInfo.Compatibility = mode
	sr.subjects.Store(subject, subjectInfo)
	return nil
}

func (sr *SchemaRegistry) GetCompatibility(ctx context.Context, subject string) (CompatibilityMode, error) {
	if subject == "" {
		return sr.config.DefaultCompatibility, nil
	}

	if s, ok := sr.subjects.Load(subject); ok {
		return s.(*Subject).Compatibility, nil
	}
	return sr.config.DefaultCompatibility, nil
}

func (sr *SchemaRegistry) getOrCreateSubject(subject string) *Subject {
	if s, ok := sr.subjects.Load(subject); ok {
		return s.(*Subject)
	}

	newSubject := &Subject{
		Name:          subject,
		Compatibility: sr.config.DefaultCompatibility,
		CreatedAt:     time.Now().UnixMilli(),
		UpdatedAt:     time.Now().UnixMilli(),
	}
	sr.subjects.Store(subject, newSubject)
	return newSubject
}

func (sr *SchemaRegistry) findByFingerprint(subject, fingerprint string) *Schema {
	var found *Schema
	sr.schemas.Range(func(key, value interface{}) bool {
		if s, ok := value.(*Schema); ok && s.Subject == subject && !s.Deleted {
			if validator, ok := sr.validators[s.SchemaType]; ok {
				fp, err := validator.GetFingerprint(s.Schema)
				if err == nil && fp == fingerprint {
					found = s
					return false
				}
			}
		}
		return true
	})
	return found
}

func (sr *SchemaRegistry) ValidateMessage(ctx context.Context, subject string, version int, message []byte) error {
	schema, err := sr.GetSchemaBySubject(ctx, subject, version)
	if err != nil {
		return err
	}

	validator, ok := sr.validators[schema.SchemaType]
	if !ok {
		return fmt.Errorf("unsupported schema type: %s", schema.SchemaType)
	}

	return validator.Validate(string(message))
}

func (sr *SchemaRegistry) Close() error {
	return nil
}

func schemaKey(subject string, version int) string {
	return fmt.Sprintf("%s:v%d", subject, version)
}

type AvroValidator struct{}

func NewAvroValidator() *AvroValidator {
	return &AvroValidator{}
}

func (v *AvroValidator) Validate(schema string) error {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		return fmt.Errorf("invalid avro schema: %w", err)
	}

	if _, ok := parsed["type"]; !ok {
		return fmt.Errorf("avro schema must have 'type' field")
	}

	return nil
}

func (v *AvroValidator) ValidateCompatibility(oldSchema, newSchema string, mode CompatibilityMode) error {
	switch mode {
	case CompatibilityNone:
		return nil
	case CompatibilityBackward, CompatibilityBackwardTransitive:
		return v.checkBackwardCompatibility(oldSchema, newSchema)
	case CompatibilityForward, CompatibilityForwardTransitive:
		return v.checkForwardCompatibility(oldSchema, newSchema)
	case CompatibilityFull, CompatibilityFullTransitive:
		if err := v.checkBackwardCompatibility(oldSchema, newSchema); err != nil {
			return err
		}
		return v.checkForwardCompatibility(oldSchema, newSchema)
	default:
		return nil
	}
}

func (v *AvroValidator) checkBackwardCompatibility(oldSchema, newSchema string) error {
	var oldParsed, newParsed map[string]interface{}
	json.Unmarshal([]byte(oldSchema), &oldParsed)
	json.Unmarshal([]byte(newSchema), &newParsed)

	oldFields := v.extractFields(oldParsed)
	newFields := v.extractFields(newParsed)

	for name, oldType := range oldFields {
		if newType, ok := newFields[name]; ok {
			if !v.typesCompatible(oldType, newType) {
				return fmt.Errorf("field '%s' type changed from %v to %v", name, oldType, newType)
			}
		} else {
			if !v.hasDefault(oldParsed, name) {
				return fmt.Errorf("field '%s' removed without default value", name)
			}
		}
	}

	return nil
}

func (v *AvroValidator) checkForwardCompatibility(oldSchema, newSchema string) error {
	var oldParsed, newParsed map[string]interface{}
	json.Unmarshal([]byte(oldSchema), &oldParsed)
	json.Unmarshal([]byte(newSchema), &newParsed)

	oldFields := v.extractFields(oldParsed)
	newFields := v.extractFields(newParsed)

	for name := range newFields {
		if _, ok := oldFields[name]; !ok {
			if !v.hasDefault(newParsed, name) {
				return fmt.Errorf("new field '%s' added without default value", name)
			}
		}
	}

	return nil
}

func (v *AvroValidator) extractFields(schema map[string]interface{}) map[string]interface{} {
	fields := make(map[string]interface{})

	if schemaType, ok := schema["type"].(string); ok && schemaType == "record" {
		if fieldList, ok := schema["fields"].([]interface{}); ok {
			for _, f := range fieldList {
				if field, ok := f.(map[string]interface{}); ok {
					if name, ok := field["name"].(string); ok {
						fields[name] = field["type"]
					}
				}
			}
		}
	}

	return fields
}

func (v *AvroValidator) hasDefault(schema map[string]interface{}, fieldName string) bool {
	if fieldList, ok := schema["fields"].([]interface{}); ok {
		for _, f := range fieldList {
			if field, ok := f.(map[string]interface{}); ok {
				if name, ok := field["name"].(string); ok && name == fieldName {
					_, hasDefault := field["default"]
					return hasDefault
				}
			}
		}
	}
	return false
}

func (v *AvroValidator) typesCompatible(oldType, newType interface{}) bool {
	if oldType == newType {
		return true
	}

	oldStr, oldOk := oldType.(string)
	newStr, newOk := newType.(string)
	if oldOk && newOk {
		return oldStr == newStr
	}

	return false
}

func (v *AvroValidator) Canonicalize(schema string) (string, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		return "", err
	}

	canonical, err := json.Marshal(parsed)
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}

func (v *AvroValidator) GetFingerprint(schema string) (string, error) {
	canonical, err := v.Canonicalize(schema)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("avro-%x", hashString(canonical)), nil
}

type ProtobufValidator struct{}

func NewProtobufValidator() *ProtobufValidator {
	return &ProtobufValidator{}
}

func (v *ProtobufValidator) Validate(schema string) error {
	if strings.TrimSpace(schema) == "" {
		return fmt.Errorf("empty protobuf schema")
	}
	return nil
}

func (v *ProtobufValidator) ValidateCompatibility(oldSchema, newSchema string, mode CompatibilityMode) error {
	return nil
}

func (v *ProtobufValidator) Canonicalize(schema string) (string, error) {
	return strings.TrimSpace(schema), nil
}

func (v *ProtobufValidator) GetFingerprint(schema string) (string, error) {
	canonical, _ := v.Canonicalize(schema)
	return fmt.Sprintf("proto-%x", hashString(canonical)), nil
}

type JSONValidator struct{}

func NewJSONValidator() *JSONValidator {
	return &JSONValidator{}
}

func (v *JSONValidator) Validate(schema string) error {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		return fmt.Errorf("invalid json schema: %w", err)
	}
	return nil
}

func (v *JSONValidator) ValidateCompatibility(oldSchema, newSchema string, mode CompatibilityMode) error {
	return nil
}

func (v *JSONValidator) Canonicalize(schema string) (string, error) {
	var parsed interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(parsed)
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}

func (v *JSONValidator) GetFingerprint(schema string) (string, error) {
	canonical, err := v.Canonicalize(schema)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("json-%x", hashString(canonical)), nil
}

func hashString(s string) uint32 {
	h := uint32(0)
	for _, c := range s {
		h = h*31 + uint32(c)
	}
	return h
}
