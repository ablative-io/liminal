//// Schema metadata helpers for Gleam-defined liminal message types.
////
//// Gleam 1.17 does not provide compiler macros or native type classes that can
//// automatically inspect a record declaration from library code. The SDK uses a
//// type-class-like provider instead: a message module exports a `Schema(Message)`
//// value, and `derive_schema` turns that provider into the Rust-SDK-compatible
//// metadata consumed by liminal. The provider is parameterised by the message
//// type, so schema declarations are checked by the Gleam compiler and require no
//// external JSON/YAML schema file.

import gleam/int
import gleam/list
import gleam/string
import liminal/channel

pub type SchemaMetadata =
  channel.SchemaMetadata

pub type SchemaField =
  channel.SchemaField

/// Compile-time schema provider for a Gleam record message type.
///
/// SDK users define one provider per record type, for example:
///
/// ```gleam
/// pub fn person_schema() -> schema.Schema(Person) {
///   schema.record(name: "Person", version: "1", fields: [
///     schema.string_field(name: "name"),
///     schema.int_field(name: "age"),
///   ])
/// }
/// ```
pub opaque type Schema(message) {
  Schema(metadata: SchemaMetadata)
}

/// Derive schema metadata from a typed Gleam schema provider.
pub fn derive_schema(schema: Schema(message)) -> SchemaMetadata {
  schema.metadata
}

/// Build a typed schema provider for a Gleam record type.
pub fn record(
  name name: String,
  version version: String,
  fields fields: List(SchemaField),
) -> Schema(message) {
  Schema(metadata: record_schema(name:, version:, fields:))
}

/// Build schema metadata for a Gleam record type without an external schema file.
///
/// The encoded schema is a JSON object compatible with the Rust SDK's
/// `SchemaMetadata.schema` bytes: it carries the object type, required field
/// names, and field type metadata.
pub fn record_schema(
  name name: String,
  version version: String,
  fields fields: List(SchemaField),
) -> SchemaMetadata {
  channel.SchemaMetadata(
    name: name,
    version: version,
    fields: fields,
    encoded_schema: encode_record_schema(fields),
  )
}

/// Construct a schema field with a Gleam field name and Gleam type name.
pub fn field(name name: String, field_type field_type: String) -> SchemaField {
  channel.SchemaField(name: name, field_type: field_type)
}

/// Construct a `String` schema field.
pub fn string_field(name name: String) -> SchemaField {
  field(name:, field_type: "String")
}

/// Construct an `Int` schema field.
pub fn int_field(name name: String) -> SchemaField {
  field(name:, field_type: "Int")
}

/// Construct a `Bool` schema field.
pub fn bool_field(name name: String) -> SchemaField {
  field(name:, field_type: "Bool")
}

/// Construct a `Float` schema field.
pub fn float_field(name name: String) -> SchemaField {
  field(name:, field_type: "Float")
}

fn encode_record_schema(fields: List(SchemaField)) -> String {
  let required =
    fields
    |> list.map(fn(field) { json_string(field.name) })
    |> string.join(with: ",")

  let properties =
    fields
    |> list.map(encode_field_property)
    |> string.join(with: ",")

  string.concat([
    "{\"type\":\"object\",\"required\":[",
    required,
    "],\"properties\":{",
    properties,
    "}}",
  ])
}

fn encode_field_property(field: SchemaField) -> String {
  string.concat([
    json_string(field.name),
    ":{\"type\":",
    json_string(field.field_type),
    "}",
  ])
}

fn json_string(value: String) -> String {
  string.concat([
    "\"",
    value
      |> string.to_utf_codepoints
      |> list.map(escape_codepoint)
      |> string.join(with: ""),
    "\"",
  ])
}

fn escape_codepoint(codepoint) -> String {
  let ordinal = string.utf_codepoint_to_int(codepoint)

  case ordinal {
    8 -> "\\b"
    9 -> "\\t"
    10 -> "\\n"
    12 -> "\\f"
    13 -> "\\r"
    34 -> "\\\""
    92 -> "\\\\"
    value if value < 32 -> "\\u00" <> two_digit_hex(value)
    _ -> string.from_utf_codepoints([codepoint])
  }
}

fn two_digit_hex(value: Int) -> String {
  let hex = int.to_base16(value)

  case string.length(hex) {
    1 -> "0" <> hex
    _ -> hex
  }
}
