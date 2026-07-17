use std::cmp::Ordering;

use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum JcsError {
    #[error("JSON decode failed: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("floating-point JSON numbers are outside the demo canonical subset")]
    FloatingPointNumber,
    #[error("JSON bytes are not canonical JCS bytes")]
    NonCanonical,
}

pub fn to_jcs_bytes(value: &Value) -> Result<Vec<u8>, JcsError> {
    let mut output = Vec::new();
    write_value(value, &mut output)?;
    Ok(output)
}

pub fn require_canonical(bytes: &[u8]) -> Result<Value, JcsError> {
    let value: Value = serde_json::from_slice(bytes)?;
    if to_jcs_bytes(&value)? == bytes {
        Ok(value)
    } else {
        Err(JcsError::NonCanonical)
    }
}

fn write_value(value: &Value, output: &mut Vec<u8>) -> Result<(), JcsError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(boolean) => output.extend_from_slice(if *boolean { b"true" } else { b"false" }),
        Value::Number(number) => {
            if number.is_f64() {
                return Err(JcsError::FloatingPointNumber);
            }
            output.extend_from_slice(number.to_string().as_bytes());
        }
        Value::String(string) => {
            output.extend_from_slice(serde_json::to_string(string)?.as_bytes());
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, item) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_value(item, output)?;
            }
            output.push(b']');
        }
        Value::Object(object) => {
            output.push(b'{');
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_by(|(left, _), (right, _)| compare_utf16(left, right));
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                output.extend_from_slice(serde_json::to_string(key)?.as_bytes());
                output.push(b':');
                write_value(item, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn compare_utf16(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

#[cfg(test)]
mod tests {
    use super::{require_canonical, to_jcs_bytes};

    #[test]
    fn sorts_object_keys_without_whitespace() -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::json!({"z": 1, "a": {"y": 2, "b": 3}});
        let bytes = to_jcs_bytes(&value)?;
        assert_eq!(bytes, br#"{"a":{"b":3,"y":2},"z":1}"#);
        require_canonical(&bytes)?;
        assert!(require_canonical(br#"{"z":1,"a":2}"#).is_err());
        Ok(())
    }
}
