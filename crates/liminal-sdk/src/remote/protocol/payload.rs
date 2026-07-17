use alloc::format;
use alloc::vec::Vec;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::SdkError;

pub(in crate::remote) fn serialize_payload<M>(message: &M) -> Result<Vec<u8>, SdkError>
where
    M: Serialize,
{
    serde_json::to_vec(message).map_err(|source| SdkError::Serialization {
        description: format!("failed to encode remote payload: {source}"),
    })
}

pub(in crate::remote) fn deserialize_payload<M>(payload: &[u8]) -> Result<M, SdkError>
where
    M: DeserializeOwned,
{
    serde_json::from_slice(payload).map_err(|source| SdkError::Serialization {
        description: format!("failed to decode remote reply payload: {source}"),
    })
}
