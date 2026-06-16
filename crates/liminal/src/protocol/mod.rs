pub mod backpressure;
pub mod causal;
pub mod codec;
pub mod envelope;
pub mod error;
pub mod frame;
pub mod handshake;
pub mod lifecycle;
pub mod multiplex;
pub mod schema;
pub mod version;

pub use codec::{decode, encode, encoded_len};
pub use error::ProtocolError;
pub use frame::{Frame, FrameHeader, FrameType, validate_stream};
pub use version::{ProtocolVersion, negotiate_version};
