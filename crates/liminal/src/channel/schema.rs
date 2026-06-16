use std::collections::BTreeMap;
use std::sync::Arc;

use jsonschema::Validator;
use serde_json::{Map, Value};
use uuid::Uuid;

/// Unique identifier for a schema version that validated a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SchemaId(Uuid);

impl SchemaId {
    /// Generates a new schema identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps an existing UUID as a schema identifier.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for SchemaId {
    fn default() -> Self {
        Self::new()
    }
}

/// JSON Schema-backed message contract for a channel.
#[derive(Clone, Debug)]
pub struct Schema {
    id: SchemaId,
    definition: Arc<Value>,
    validator: Arc<Validator>,
    defaults: Arc<BTreeMap<String, Value>>,
}

impl Schema {
    /// Builds a schema from a JSON Schema definition.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidationError::InvalidSchema`] when the definition cannot be compiled.
    pub fn new(definition: Value) -> Result<Self, SchemaValidationError> {
        Self::with_id(SchemaId::new(), definition)
    }

    /// Builds a schema from a JSON Schema definition with an explicit identifier.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidationError::InvalidSchema`] when the definition cannot be compiled.
    pub fn with_id(id: SchemaId, definition: Value) -> Result<Self, SchemaValidationError> {
        let defaults = collect_object_defaults(&definition)?;
        Self::from_parts(id, definition, defaults)
    }

    /// Returns the schema version identifier.
    #[must_use]
    pub const fn id(&self) -> SchemaId {
        self.id
    }

    /// Returns the wrapped JSON Schema definition.
    #[must_use]
    pub fn definition(&self) -> &Value {
        &self.definition
    }

    /// Validates JSON payload bytes after applying known schema defaults.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidationError`] when the payload is not JSON, cannot receive required
    /// defaults, or does not match this schema.
    pub fn validate<Payload>(&self, payload: Payload) -> Result<(), SchemaValidationError>
    where
        Payload: AsRef<[u8]>,
    {
        let value = self.normalized_value(payload)?;
        self.validate_value(&value)
    }

    /// Validates payload bytes and returns the normalized JSON bytes to deliver.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidationError`] when the payload cannot be parsed, normalized, validated,
    /// or serialized for delivery.
    pub fn validate_and_apply_defaults<Payload>(
        &self,
        payload: Payload,
    ) -> Result<Vec<u8>, SchemaValidationError>
    where
        Payload: AsRef<[u8]>,
    {
        let value = self.normalized_value(payload)?;
        self.validate_value(&value)?;
        serde_json::to_vec(&value).map_err(|source| SchemaValidationError::Serialize { source })
    }

    /// Evolves an object schema by adding a required field with a default value.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidationError`] when this is not an object schema, the field schema is
    /// invalid, the default does not match the field schema, or the evolved schema cannot compile.
    pub fn evolve_add_field(
        &self,
        name: impl Into<String>,
        field_schema: Value,
        default: Value,
    ) -> Result<Self, SchemaValidationError> {
        let name = name.into();
        if name.is_empty() {
            return Err(SchemaValidationError::EmptyFieldName);
        }
        validate_default(&field_schema, &default)?;

        let mut definition = (*self.definition).clone();
        if !is_object_schema(&definition) {
            return Err(SchemaValidationError::NotObjectSchema);
        }

        let document = definition
            .as_object_mut()
            .ok_or(SchemaValidationError::NotObjectSchema)?;
        insert_property_schema(document, &name, field_schema, &default)?;
        insert_required_field(document, &name)?;

        let mut defaults = (*self.defaults).clone();
        defaults.insert(name, default);
        Self::from_parts(SchemaId::new(), definition, defaults)
    }

    fn from_parts(
        id: SchemaId,
        definition: Value,
        defaults: BTreeMap<String, Value>,
    ) -> Result<Self, SchemaValidationError> {
        let validator = jsonschema::validator_for(&definition).map_err(|error| {
            SchemaValidationError::InvalidSchema {
                message: error.to_string(),
            }
        })?;

        Ok(Self {
            id,
            definition: Arc::new(definition),
            validator: Arc::new(validator),
            defaults: Arc::new(defaults),
        })
    }

    fn normalized_value<Payload>(&self, payload: Payload) -> Result<Value, SchemaValidationError>
    where
        Payload: AsRef<[u8]>,
    {
        let mut value = serde_json::from_slice(payload.as_ref())
            .map_err(|source| SchemaValidationError::InvalidJson { source })?;
        self.apply_defaults(&mut value)?;
        Ok(value)
    }

    fn apply_defaults(&self, value: &mut Value) -> Result<(), SchemaValidationError> {
        if self.defaults.is_empty() {
            return Ok(());
        }

        let object = value
            .as_object_mut()
            .ok_or(SchemaValidationError::PayloadNotObject)?;
        for (field, default) in self.defaults.iter() {
            object
                .entry(field.clone())
                .or_insert_with(|| default.clone());
        }
        Ok(())
    }

    fn validate_value(&self, value: &Value) -> Result<(), SchemaValidationError> {
        self.validator
            .validate(value)
            .map_err(|error| SchemaValidationError::Mismatch {
                message: error.to_string(),
            })
    }
}

/// Errors returned while compiling schemas, validating payloads, or evolving schemas.
#[derive(Debug, thiserror::Error)]
pub enum SchemaValidationError {
    /// The payload was not syntactically valid JSON.
    #[error("invalid JSON payload: {source}")]
    InvalidJson { source: serde_json::Error },
    /// The JSON Schema definition could not be compiled.
    #[error("invalid JSON Schema: {message}")]
    InvalidSchema { message: String },
    /// The payload did not satisfy the JSON Schema definition.
    #[error("payload does not match schema: {message}")]
    Mismatch { message: String },
    /// Schema evolution can only add fields to object schemas.
    #[error("schema evolution only supports object schemas")]
    NotObjectSchema,
    /// Schema evolution field names must be non-empty.
    #[error("schema evolution field name must not be empty")]
    EmptyFieldName,
    /// The schema's properties member was not an object.
    #[error("object schema properties must be an object")]
    InvalidProperties,
    /// The schema's required member was not an array of strings.
    #[error("object schema required must be an array of strings")]
    InvalidRequired,
    /// The payload cannot receive object-field defaults because it is not an object.
    #[error("payload must be a JSON object to apply schema defaults")]
    PayloadNotObject,
    /// The normalized JSON payload could not be serialized for delivery.
    #[error("failed to serialize normalized payload: {source}")]
    Serialize { source: serde_json::Error },
}

fn collect_object_defaults(
    definition: &Value,
) -> Result<BTreeMap<String, Value>, SchemaValidationError> {
    let Some(document) = definition.as_object() else {
        return Ok(BTreeMap::new());
    };
    let Some(properties) = document.get("properties") else {
        return Ok(BTreeMap::new());
    };
    let properties = properties
        .as_object()
        .ok_or(SchemaValidationError::InvalidProperties)?;

    let defaults = properties
        .iter()
        .filter_map(|(field, schema)| {
            schema
                .as_object()
                .and_then(|field_schema| field_schema.get("default"))
                .map(|default| (field.clone(), default.clone()))
        })
        .collect();
    Ok(defaults)
}

fn validate_default(field_schema: &Value, default: &Value) -> Result<(), SchemaValidationError> {
    let validator = jsonschema::validator_for(field_schema).map_err(|error| {
        SchemaValidationError::InvalidSchema {
            message: error.to_string(),
        }
    })?;
    validator
        .validate(default)
        .map_err(|error| SchemaValidationError::Mismatch {
            message: format!("default value does not match field schema: {error}"),
        })
}

fn is_object_schema(definition: &Value) -> bool {
    let Some(document) = definition.as_object() else {
        return false;
    };

    match document.get("type") {
        Some(Value::String(schema_type)) => schema_type == "object",
        Some(Value::Array(schema_types)) => schema_types
            .iter()
            .any(|schema_type| schema_type.as_str() == Some("object")),
        Some(_) => false,
        None => document.contains_key("properties"),
    }
}

fn insert_property_schema(
    document: &mut Map<String, Value>,
    name: &str,
    mut field_schema: Value,
    default: &Value,
) -> Result<(), SchemaValidationError> {
    let field_document =
        field_schema
            .as_object_mut()
            .ok_or_else(|| SchemaValidationError::InvalidSchema {
                message: "field schema must be a JSON Schema object".to_owned(),
            })?;
    field_document.insert("default".to_owned(), default.clone());

    let properties = document
        .entry("properties".to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    let properties = properties
        .as_object_mut()
        .ok_or(SchemaValidationError::InvalidProperties)?;
    properties.insert(name.to_owned(), field_schema);
    Ok(())
}

fn insert_required_field(
    document: &mut Map<String, Value>,
    name: &str,
) -> Result<(), SchemaValidationError> {
    let required = document
        .entry("required".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    let required = required
        .as_array_mut()
        .ok_or(SchemaValidationError::InvalidRequired)?;

    if required.iter().any(|item| item.as_str().is_none()) {
        return Err(SchemaValidationError::InvalidRequired);
    }
    if !required.iter().any(|item| item.as_str() == Some(name)) {
        required.push(Value::String(name.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Schema, SchemaId, SchemaValidationError};
    use serde_json::{Value, json};

    #[test]
    fn schema_is_clone_send_sync() {
        fn assert_bounds<T: Clone + Send + Sync + std::fmt::Debug>() {}

        assert_bounds::<Schema>();
    }

    #[test]
    fn validates_payload_against_json_schema() -> Result<(), SchemaValidationError> {
        let schema = order_schema()?;

        schema.validate(br#"{"order_id":"A1","quantity":3}"#)?;
        let result = schema.validate(br#"{"order_id":"A1","quantity":0}"#);

        assert!(matches!(
            result,
            Err(SchemaValidationError::Mismatch { .. })
        ));
        Ok(())
    }

    #[test]
    fn evolution_adds_defaulted_field_and_changes_schema_id() -> Result<(), SchemaValidationError> {
        let schema = order_schema()?;
        let old_id = schema.id();
        let evolved =
            schema.evolve_add_field("priority", json!({"type":"string"}), json!("normal"))?;
        let normalized =
            evolved.validate_and_apply_defaults(br#"{"order_id":"A1","quantity":3}"#)?;
        let payload: Value = serde_json::from_slice(&normalized)
            .map_err(|source| SchemaValidationError::InvalidJson { source })?;

        assert_ne!(evolved.id(), old_id);
        assert_eq!(payload.get("priority"), Some(&json!("normal")));
        Ok(())
    }

    #[test]
    fn evolution_rejects_non_object_schema() -> Result<(), SchemaValidationError> {
        let schema = Schema::new(json!({"type":"array"}))?;
        let result = schema.evolve_add_field("priority", json!({"type":"string"}), json!("normal"));

        assert!(matches!(
            result,
            Err(SchemaValidationError::NotObjectSchema)
        ));
        Ok(())
    }

    #[test]
    fn explicit_schema_id_is_preserved() -> Result<(), SchemaValidationError> {
        let id = SchemaId::new();
        let schema = Schema::with_id(id, json!({"type":"object"}))?;

        assert_eq!(schema.id(), id);
        Ok(())
    }

    fn order_schema() -> Result<Schema, SchemaValidationError> {
        Schema::new(json!({
            "type": "object",
            "properties": {
                "order_id": {"type": "string"},
                "quantity": {"type": "integer", "minimum": 1}
            },
            "required": ["order_id", "quantity"],
            "additionalProperties": false
        }))
    }
}
