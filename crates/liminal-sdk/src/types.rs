use alloc::borrow::Cow;

/// Schema metadata supplied by user-defined message types.
///
/// The SDK uses this metadata to let concrete channel implementations validate
/// outbound payloads against the channel's declared type. The trait that returns
/// this value is intentionally metadata-only: it does not parse schemas or run
/// validation itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaMetadata {
    /// Stable message schema name.
    pub name: Cow<'static, str>,
    /// Stable message schema version.
    pub version: Cow<'static, str>,
    /// Encoded schema definition bytes.
    pub schema: Cow<'static, [u8]>,
}

impl SchemaMetadata {
    /// Creates schema metadata from a name, version, and schema bytes.
    #[must_use]
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        version: impl Into<Cow<'static, str>>,
        schema: impl Into<Cow<'static, [u8]>>,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            schema: schema.into(),
        }
    }
}

/// Compile-time marker for message types that declare schema metadata.
///
/// Publishing methods require this trait in addition to [`serde::Serialize`].
/// That bound ensures a type that is merely serializable cannot be published to
/// a typed liminal channel unless it also declares the schema metadata needed by
/// the bus for publish-time validation.
///
/// Implement the trait manually today:
///
/// ```
/// use liminal_sdk::{SchemaMetadata, SchemaValidate};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Created {
///     id: String,
/// }
///
/// impl SchemaValidate for Created {
///     fn schema_metadata() -> SchemaMetadata {
///         SchemaMetadata::new(
///             "example.created",
///             "1",
///             br#"{"type":"object","required":["id"]}"#.as_slice(),
///         )
///     }
/// }
/// ```
///
/// A future `#[derive(SchemaValidate)]` macro is expected to generate this same
/// implementation for supported schema-generation workflows. This crate does
/// not provide a blanket implementation for all serializable types because doing
/// so would remove the compile-time enforcement required by typed channels.
pub trait SchemaValidate {
    /// Returns this message type's schema metadata.
    #[must_use]
    fn schema_metadata() -> SchemaMetadata;
}
