use serde_json::{Map, Value};

use crate::jcs::{JcsError, require_canonical, to_jcs_bytes};

const HEADER_LENGTH_BYTES: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractId {
    namespace: String,
    name: String,
    version: u64,
}

impl ContractId {
    pub fn new(namespace: &str, name: &str, version: u64) -> Result<Self, EnvelopeError> {
        if !is_contract_component(namespace) || !is_contract_component(name) || version == 0 {
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
}

fn is_contract_component(value: &str) -> bool {
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
    pub contract_id: ContractId,
    pub generation: u64,
    pub seq: u64,
    pub kind: FrameKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedEnvelope {
    pub header: EnvelopeHeader,
    pub state: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("invalid component contract id {0:?}")]
    InvalidContractId(String),
    #[error("invalid component frame kind {0:?}")]
    InvalidKind(String),
    #[error("envelope frame is shorter than its length prefix")]
    Truncated,
    #[error("envelope header is too large")]
    HeaderTooLarge,
    #[error("envelope header field {0:?} is absent or has the wrong type")]
    InvalidHeaderField(&'static str),
    #[error("envelope header has unknown fields")]
    UnknownHeaderFields,
    #[error("envelope header/state JSON failed canonical validation: {0}")]
    Jcs(#[from] JcsError),
}

/// Frame-owned seam for the canonical envelope prefix and state-byte split.
///
/// `PlaceholderEnvelopeCodec` is the only placeholder wire-layout binding. Replace
/// that implementation here with the frame worker's committed typed-module and
/// JSON-schema byte layout; cadence, graph generation, and transport remain unchanged.
pub trait EnvelopeCodec {
    fn encode(&self, header: &EnvelopeHeader, state: &[u8]) -> Result<Vec<u8>, EnvelopeError>;

    fn decode(&self, frame: &[u8]) -> Result<DecodedEnvelope, EnvelopeError>;
}

/// Placeholder layout: four-byte big-endian header length, canonical header JSON,
/// then canonical pure-state JSON. This is intentionally isolated behind
/// `EnvelopeCodec` and is not asserted as the final frame-owned encoding.
#[derive(Clone, Copy, Debug)]
pub struct PlaceholderEnvelopeCodec;

impl EnvelopeCodec for PlaceholderEnvelopeCodec {
    fn encode(&self, header: &EnvelopeHeader, state: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
        require_canonical(state)?;
        let header_bytes = encode_header(header)?;
        let header_length =
            u32::try_from(header_bytes.len()).map_err(|_| EnvelopeError::HeaderTooLarge)?;
        let mut frame = Vec::with_capacity(HEADER_LENGTH_BYTES + header_bytes.len() + state.len());
        frame.extend_from_slice(&header_length.to_be_bytes());
        frame.extend_from_slice(&header_bytes);
        frame.extend_from_slice(state);
        Ok(frame)
    }

    fn decode(&self, frame: &[u8]) -> Result<DecodedEnvelope, EnvelopeError> {
        let length_bytes = frame
            .get(..HEADER_LENGTH_BYTES)
            .ok_or(EnvelopeError::Truncated)?;
        let length_array: [u8; HEADER_LENGTH_BYTES] = length_bytes
            .try_into()
            .map_err(|_| EnvelopeError::Truncated)?;
        let header_length = u32::from_be_bytes(length_array) as usize;
        let header_end = HEADER_LENGTH_BYTES
            .checked_add(header_length)
            .ok_or(EnvelopeError::HeaderTooLarge)?;
        let header_bytes = frame
            .get(HEADER_LENGTH_BYTES..header_end)
            .ok_or(EnvelopeError::Truncated)?;
        let state = frame.get(header_end..).ok_or(EnvelopeError::Truncated)?;
        require_canonical(header_bytes)?;
        require_canonical(state)?;
        Ok(DecodedEnvelope {
            header: decode_header(header_bytes)?,
            state: state.to_vec(),
        })
    }
}

fn encode_header(header: &EnvelopeHeader) -> Result<Vec<u8>, EnvelopeError> {
    let mut object = Map::new();
    object.insert(
        "contract-id".to_owned(),
        Value::String(header.contract_id.canonical()),
    );
    object.insert("generation".to_owned(), Value::from(header.generation));
    object.insert(
        "kind".to_owned(),
        Value::String(header.kind.as_str().to_owned()),
    );
    object.insert("seq".to_owned(), Value::from(header.seq));
    to_jcs_bytes(&Value::Object(object)).map_err(Into::into)
}

fn decode_header(bytes: &[u8]) -> Result<EnvelopeHeader, EnvelopeError> {
    let Value::Object(mut object) = require_canonical(bytes)? else {
        return Err(EnvelopeError::InvalidHeaderField("header"));
    };
    let contract_id = take_string(&mut object, "contract-id")?;
    let generation = take_u64(&mut object, "generation")?;
    let kind = take_string(&mut object, "kind")?;
    let seq = take_u64(&mut object, "seq")?;
    if !object.is_empty() {
        return Err(EnvelopeError::UnknownHeaderFields);
    }
    Ok(EnvelopeHeader {
        contract_id: ContractId::parse(&contract_id)?,
        generation,
        seq,
        kind: FrameKind::parse(&kind)?,
    })
}

fn take_string(
    object: &mut Map<String, Value>,
    key: &'static str,
) -> Result<String, EnvelopeError> {
    object
        .remove(key)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or(EnvelopeError::InvalidHeaderField(key))
}

fn take_u64(object: &mut Map<String, Value>, key: &'static str) -> Result<u64, EnvelopeError> {
    object
        .remove(key)
        .and_then(|value| value.as_u64())
        .ok_or(EnvelopeError::InvalidHeaderField(key))
}

#[cfg(test)]
mod tests {
    use super::{ContractId, EnvelopeCodec, EnvelopeHeader, FrameKind, PlaceholderEnvelopeCodec};

    fn header() -> Result<EnvelopeHeader, Box<dyn std::error::Error>> {
        Ok(EnvelopeHeader {
            contract_id: ContractId::new("frame", "graph-view", 1)?,
            generation: 4,
            seq: 9,
            kind: FrameKind::Delta,
        })
    }

    #[test]
    fn codec_round_trips_header_and_state() -> Result<(), Box<dyn std::error::Error>> {
        let codec = PlaceholderEnvelopeCodec;
        let header = header()?;
        let state = br#"{"edges":[],"nodes":[]}"#;
        let encoded = codec.encode(&header, state)?;
        let decoded = codec.decode(&encoded)?;
        assert_eq!(decoded.header, header);
        assert_eq!(decoded.state, state);
        Ok(())
    }

    #[test]
    fn codec_rejects_non_canonical_header_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let codec = PlaceholderEnvelopeCodec;
        let canonical = codec.encode(&header()?, br#"{"edges":[],"nodes":[]}"#)?;
        let state_offset = 4 + usize::try_from(u32::from_be_bytes([
            canonical[0],
            canonical[1],
            canonical[2],
            canonical[3],
        ]))?;
        let state = &canonical[state_offset..];
        let non_canonical =
            br#"{"seq":9,"kind":"delta","generation":4,"contract-id":"frame:graph-view@v1"}"#;
        let length = u32::try_from(non_canonical.len())?;
        let mut frame = length.to_be_bytes().to_vec();
        frame.extend_from_slice(non_canonical);
        frame.extend_from_slice(state);
        assert!(codec.decode(&frame).is_err());
        Ok(())
    }

    #[test]
    fn contract_id_enforces_ascii_grammar() {
        assert!(ContractId::new("frame", "graph-view", 1).is_ok());
        assert!(ContractId::new("Frame", "graph_view", 1).is_err());
        assert!(ContractId::new("frame", "graph-view", 0).is_err());
    }
}
