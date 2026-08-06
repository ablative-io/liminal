//! Channel schema resolution: turns a configured channel's loaded schema (or the
//! permissive empty default) into the JSON Schema document the channel is built
//! with and the protocol schema id advertised at subscribe time.

use liminal::protocol::SchemaId as ProtocolSchemaId;
use serde_json::Value;

use crate::config::types::ChannelDef;

/// A resolved channel schema: the JSON Schema document plus the protocol schema id
/// advertised to subscribers.
pub(super) struct ChannelSchema {
    /// JSON Schema document fed to the channel's validation engine.
    pub document: Value,
    /// Protocol schema id derived from the schema bytes, advertised on subscribe.
    pub protocol_id: ProtocolSchemaId,
}

/// Canonical bytes of the permissive empty schema. A schema-less channel derives
/// its protocol id from these bytes so the id is stable across restarts.
const EMPTY_SCHEMA_BYTES: &[u8] = b"{}";

/// Resolves the JSON Schema document and protocol schema id for a channel.
///
/// A channel whose `schema_ref` was loaded during config validation uses that
/// parsed document and derives its protocol id from the RAW loaded schema bytes. A
/// channel with no loaded schema (either `schema_ref: None`, or a config that was
/// never validated) keeps the permissive empty schema `{}` that accepts any JSON
/// payload, deriving its protocol id from the `{}` bytes.
pub(super) fn resolve_channel_schema(channel: &ChannelDef) -> ChannelSchema {
    channel.loaded_schema.as_ref().map_or_else(
        || ChannelSchema {
            document: Value::Object(serde_json::Map::new()),
            protocol_id: schema_id_from_bytes(EMPTY_SCHEMA_BYTES),
        },
        |loaded| ChannelSchema {
            document: loaded.document.clone(),
            protocol_id: schema_id_from_bytes(&loaded.bytes),
        },
    )
}

/// Resolves the JSON Schema document and protocol schema id from RAW schema
/// bytes.
///
/// This is [`resolve_channel_schema`]'s two branches lifted off [`ChannelDef`]:
/// `None` is the permissive empty schema `{}` with the id derived from the `{}`
/// bytes, and `Some(bytes)` is the parsed document with the id derived from
/// those same bytes. The only difference is WHO parses — config validation
/// already parsed a boot channel's file into `loaded_schema.document`, while a
/// runtime registration hands over bytes and this function parses them — so a
/// parse failure is surfaced here instead of at config-load time.
///
/// The boot path deliberately keeps calling [`resolve_channel_schema`]: routing
/// it through here would re-parse a document config validation already parsed,
/// which is a behaviour change (a different error site) in a step that claims
/// the boot path is untouched.
///
/// # Errors
/// Returns the `serde_json` parse error when `schema_bytes` is not valid JSON.
pub(super) fn resolve_schema_bytes(
    schema_bytes: Option<&[u8]>,
) -> Result<ChannelSchema, serde_json::Error> {
    let Some(bytes) = schema_bytes else {
        return Ok(ChannelSchema {
            document: Value::Object(serde_json::Map::new()),
            protocol_id: schema_id_from_bytes(EMPTY_SCHEMA_BYTES),
        });
    };
    Ok(ChannelSchema {
        document: serde_json::from_slice(bytes)?,
        protocol_id: schema_id_from_bytes(bytes),
    })
}

/// Derives a stable 32-byte protocol schema id from schema bytes via FNV-1a,
/// spread across the id exactly as the SDK's `schema_id_from_bytes`
/// (`liminal-sdk` remote/tcp) does, so an SDK deriving ids from the same schema
/// bytes converges on the same id.
fn schema_id_from_bytes(schema_bytes: &[u8]) -> ProtocolSchemaId {
    let mut id = [0_u8; ProtocolSchemaId::WIRE_LEN];
    let mut hash = fnv1a(schema_bytes).to_be_bytes();
    // Spread the 8-byte digest across the 32-byte id deterministically, re-hashing
    // after every full 8-byte block so the id is not just the digest repeated.
    for (index, slot) in id.iter_mut().enumerate() {
        *slot = hash[index % hash.len()];
        if index % hash.len() == hash.len() - 1 {
            hash = fnv1a(&hash).to_be_bytes();
        }
    }
    ProtocolSchemaId::new(id)
}

/// FNV-1a 64-bit hash. Mirrors the SDK's `fnv1a` so schema-id derivation matches
/// byte for byte across the server and the SDK.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::{resolve_channel_schema, resolve_schema_bytes, schema_id_from_bytes};
    use crate::config::types::{ChannelDef, LoadedSchema};

    fn channel(schema_ref: Option<&str>, loaded: Option<LoadedSchema>) -> ChannelDef {
        ChannelDef {
            name: "orders".to_owned(),
            schema_ref: schema_ref.map(Into::into),
            durable: false,
            loaded_schema: loaded,
        }
    }

    #[test]
    fn schema_ids_are_deterministic_and_content_addressed() {
        assert_eq!(schema_id_from_bytes(b"a"), schema_id_from_bytes(b"a"));
        assert_ne!(schema_id_from_bytes(b"a"), schema_id_from_bytes(b"b"));
    }

    #[test]
    fn loaded_schema_id_derives_from_loaded_bytes() {
        let bytes = br#"{"type":"object"}"#.to_vec();
        let document = serde_json::json!({"type": "object"});
        let resolved = resolve_channel_schema(&channel(
            Some("orders.json"),
            Some(LoadedSchema {
                bytes: bytes.clone(),
                document: document.clone(),
            }),
        ));

        assert_eq!(resolved.document, document);
        assert_eq!(resolved.protocol_id, schema_id_from_bytes(&bytes));
    }

    #[test]
    fn schema_less_channel_is_permissive_and_uses_empty_schema_id() {
        let resolved = resolve_channel_schema(&channel(None, None));

        assert_eq!(resolved.document, serde_json::json!({}));
        assert_eq!(resolved.protocol_id, schema_id_from_bytes(b"{}"));
    }

    /// The raw-bytes lift resolves the SAME two branches as the `ChannelDef`
    /// form: absent bytes are the permissive `{}` with the `{}` id, and present
    /// bytes give the parsed document with the id derived from those bytes. A
    /// runtime registration and a boot channel over identical schema bytes must
    /// therefore land on identical entries.
    #[test]
    fn raw_bytes_resolution_matches_the_channel_def_form() -> Result<(), serde_json::Error> {
        let empty = resolve_schema_bytes(None)?;
        let from_def = resolve_channel_schema(&channel(None, None));
        assert_eq!(empty.document, from_def.document);
        assert_eq!(empty.protocol_id, from_def.protocol_id);

        let bytes = br#"{"type":"object"}"#.to_vec();
        let loaded = resolve_schema_bytes(Some(&bytes))?;
        let loaded_from_def = resolve_channel_schema(&channel(
            Some("orders.json"),
            Some(LoadedSchema {
                bytes,
                document: serde_json::json!({"type": "object"}),
            }),
        ));
        assert_eq!(loaded.document, loaded_from_def.document);
        assert_eq!(loaded.protocol_id, loaded_from_def.protocol_id);
        Ok(())
    }

    /// Bytes that are not JSON at all are refused HERE, by the parser, rather
    /// than reaching schema compilation with a half-built document.
    #[test]
    fn raw_bytes_resolution_refuses_bytes_that_are_not_json() {
        assert!(resolve_schema_bytes(Some(b"not json")).is_err());
    }
}
