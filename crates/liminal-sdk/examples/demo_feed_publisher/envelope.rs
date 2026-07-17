use std::str;

use serde_json::{Map, Value};

use crate::jcs::{JcsError, require_canonical, to_jcs_bytes};

pub const GRAPH_VIEW_CONTRACT_ID: &str = "frame:graph-view@v1";
const MAX_SAFE_JSON_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentId(String);

impl ComponentId {
    pub fn new(value: &str) -> Result<Self, EnvelopeError> {
        if !is_slug(value) {
            return Err(EnvelopeError::InvalidComponentId(value.to_owned()));
        }
        Ok(Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractId {
    namespace: String,
    name: String,
    version: u64,
}

impl ContractId {
    pub fn new(namespace: &str, name: &str, version: u64) -> Result<Self, EnvelopeError> {
        if !is_slug(namespace) || !is_slug(name) {
            return Err(EnvelopeError::InvalidContractId(format!(
                "{namespace}:{name}@v{version}"
            )));
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            version,
        })
    }

    fn parse(value: &str) -> Result<Self, EnvelopeError> {
        let (qualified_name, version) = value
            .rsplit_once("@v")
            .ok_or_else(|| EnvelopeError::InvalidContractId(value.to_owned()))?;
        let (namespace, name) = qualified_name
            .split_once(':')
            .ok_or_else(|| EnvelopeError::InvalidContractId(value.to_owned()))?;
        let version = version
            .parse::<u64>()
            .map_err(|_| EnvelopeError::InvalidContractId(value.to_owned()))?;
        Self::new(namespace, name, version)
    }

    #[must_use]
    pub fn canonical(&self) -> String {
        format!("{}:{}@v{}", self.namespace, self.name, self.version)
    }

    fn require_graph_view_v1(&self) -> Result<(), EnvelopeError> {
        let actual = self.canonical();
        if actual == GRAPH_VIEW_CONTRACT_ID {
            Ok(())
        } else {
            Err(EnvelopeError::WrongContract(actual))
        }
    }
}

fn is_slug(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    Snapshot,
    Delta,
}

impl FrameKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Delta => "delta",
        }
    }

    fn parse(value: &str) -> Result<Self, EnvelopeError> {
        match value {
            "snapshot" => Ok(Self::Snapshot),
            "delta" => Ok(Self::Delta),
            _ => Err(EnvelopeError::InvalidKind(value.to_owned())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvelopeHeader {
    pub component_id: ComponentId,
    pub contract_id: ContractId,
    pub generation: u64,
    pub kind: FrameKind,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedEnvelope {
    pub header: EnvelopeHeader,
    pub state: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("invalid component id {0:?}")]
    InvalidComponentId(String),
    #[error("invalid component contract id {0:?}")]
    InvalidContractId(String),
    #[error("expected contract frame:graph-view@v1, got {0:?}")]
    WrongContract(String),
    #[error("invalid component frame kind {0:?}")]
    InvalidKind(String),
    #[error("envelope member {0:?} is absent or has the wrong type")]
    InvalidMember(&'static str),
    #[error("envelope must contain exactly its six ruled members")]
    UnknownMembers,
    #[error("generation must be at least one")]
    InvalidGeneration,
    #[error("generation and seq must be JavaScript-safe integers")]
    UnsafeInteger,
    #[error("canonical payload text is not UTF-8: {0}")]
    PayloadUtf8(#[from] str::Utf8Error),
    #[error("envelope or nested payload failed canonical validation: {0}")]
    Jcs(#[from] JcsError),
}

/// Frame-owned seam for the real six-member RFC-8785 envelope object.
pub trait EnvelopeCodec {
    fn encode(&self, header: &EnvelopeHeader, state: &[u8]) -> Result<Vec<u8>, EnvelopeError>;

    fn decode(&self, frame: &[u8]) -> Result<DecodedEnvelope, EnvelopeError>;
}

/// `frame:graph-view@v1` envelope codec bound to the frame demo doc-of-record.
#[derive(Clone, Copy, Debug)]
pub struct FrameEnvelopeCodec;

impl EnvelopeCodec for FrameEnvelopeCodec {
    fn encode(&self, header: &EnvelopeHeader, state: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
        validate_header(header)?;
        require_canonical(state)?;
        let payload = str::from_utf8(state)?;
        let mut object = Map::new();
        object.insert(
            "componentId".to_owned(),
            Value::String(header.component_id.as_str().to_owned()),
        );
        object.insert(
            "contractId".to_owned(),
            Value::String(header.contract_id.canonical()),
        );
        object.insert("generation".to_owned(), Value::from(header.generation));
        object.insert(
            "kind".to_owned(),
            Value::String(header.kind.as_str().to_owned()),
        );
        object.insert("payload".to_owned(), Value::String(payload.to_owned()));
        object.insert("seq".to_owned(), Value::from(header.seq));
        to_jcs_bytes(&Value::Object(object)).map_err(Into::into)
    }

    fn decode(&self, frame: &[u8]) -> Result<DecodedEnvelope, EnvelopeError> {
        let Value::Object(mut object) = require_canonical(frame)? else {
            return Err(EnvelopeError::InvalidMember("envelope"));
        };
        let component_id = ComponentId::new(&take_string(&mut object, "componentId")?)?;
        let contract_id = ContractId::parse(&take_string(&mut object, "contractId")?)?;
        let generation = take_u64(&mut object, "generation")?;
        let kind = FrameKind::parse(&take_string(&mut object, "kind")?)?;
        let payload = take_string(&mut object, "payload")?;
        let seq = take_u64(&mut object, "seq")?;
        if !object.is_empty() {
            return Err(EnvelopeError::UnknownMembers);
        }
        let header = EnvelopeHeader {
            component_id,
            contract_id,
            generation,
            kind,
            seq,
        };
        validate_header(&header)?;
        require_canonical(payload.as_bytes())?;
        Ok(DecodedEnvelope {
            header,
            state: payload.into_bytes(),
        })
    }
}

fn validate_header(header: &EnvelopeHeader) -> Result<(), EnvelopeError> {
    header.contract_id.require_graph_view_v1()?;
    if header.generation == 0 {
        return Err(EnvelopeError::InvalidGeneration);
    }
    if header.generation > MAX_SAFE_JSON_INTEGER || header.seq > MAX_SAFE_JSON_INTEGER {
        return Err(EnvelopeError::UnsafeInteger);
    }
    Ok(())
}

fn take_string(
    object: &mut Map<String, Value>,
    key: &'static str,
) -> Result<String, EnvelopeError> {
    object
        .remove(key)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or(EnvelopeError::InvalidMember(key))
}

fn take_u64(object: &mut Map<String, Value>, key: &'static str) -> Result<u64, EnvelopeError> {
    object
        .remove(key)
        .and_then(|value| value.as_u64())
        .ok_or(EnvelopeError::InvalidMember(key))
}

#[cfg(test)]
mod tests {
    use super::{
        ComponentId, ContractId, EnvelopeCodec, EnvelopeHeader, FrameEnvelopeCodec, FrameKind,
    };

    const FRAME_REFERENCE_VECTOR: &[u8] = br#"{"componentId":"demo-graph","contractId":"frame:graph-view@v1","generation":3,"kind":"delta","payload":"{\"nodeId\":\"core\",\"type\":\"value-changed\",\"value\":9}","seq":12}"#;

    fn header() -> Result<EnvelopeHeader, Box<dyn std::error::Error>> {
        Ok(EnvelopeHeader {
            component_id: ComponentId::new("demo-graph")?,
            contract_id: ContractId::new("frame", "graph-view", 1)?,
            generation: 3,
            kind: FrameKind::Delta,
            seq: 12,
        })
    }

    #[test]
    fn codec_matches_frame_reference_encoder_vector() -> Result<(), Box<dyn std::error::Error>> {
        let state = br#"{"nodeId":"core","type":"value-changed","value":9}"#;
        let encoded = FrameEnvelopeCodec.encode(&header()?, state)?;
        assert_eq!(encoded, FRAME_REFERENCE_VECTOR);
        Ok(())
    }

    #[test]
    fn envelope_is_six_member_jcs_object_with_nested_payload_string()
    -> Result<(), Box<dyn std::error::Error>> {
        let decoded = FrameEnvelopeCodec.decode(FRAME_REFERENCE_VECTOR)?;
        assert_eq!(decoded.header, header()?);
        assert_eq!(
            decoded.state,
            br#"{"nodeId":"core","type":"value-changed","value":9}"#
        );
        let text = str::from_utf8(FRAME_REFERENCE_VECTOR)?;
        assert!(text.starts_with(
            r#"{"componentId":"demo-graph","contractId":"frame:graph-view@v1","generation":3,"kind":"delta","payload":"#
        ));
        let parsed = crate::jcs::require_canonical(FRAME_REFERENCE_VECTOR)?;
        assert_eq!(parsed.as_object().map(serde_json::Map::len), Some(6));
        assert!(parsed["payload"].is_string());
        Ok(())
    }

    #[test]
    fn codec_rejects_non_canonical_envelope_bytes() {
        let non_canonical = br#"{"seq":12,"payload":"{\"nodeId\":\"core\",\"type\":\"value-changed\",\"value\":9}","kind":"delta","generation":3,"contractId":"frame:graph-view@v1","componentId":"demo-graph"}"#;
        assert!(FrameEnvelopeCodec.decode(non_canonical).is_err());
    }

    #[test]
    fn codec_rejects_non_canonical_nested_payload() -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::json!({
            "componentId": "demo-graph",
            "contractId": "frame:graph-view@v1",
            "generation": 3,
            "kind": "delta",
            "payload": "{\"value\":9,\"nodeId\":\"core\",\"type\":\"value-changed\"}",
            "seq": 12,
        });
        let bytes = crate::jcs::to_jcs_bytes(&value)?;
        assert!(FrameEnvelopeCodec.decode(&bytes).is_err());
        Ok(())
    }

    #[test]
    fn identifiers_enforce_the_real_binding() -> Result<(), Box<dyn std::error::Error>> {
        assert!(ComponentId::new("graph-view-demo").is_ok());
        assert!(ComponentId::new("Graph_view").is_err());
        let wrong = EnvelopeHeader {
            contract_id: ContractId::new("frame", "other", 1)?,
            ..header()?
        };
        assert!(
            FrameEnvelopeCodec
                .encode(&wrong, br#"{"nodeId":"core","type":"node-removed"}"#)
                .is_err()
        );
        Ok(())
    }
}
