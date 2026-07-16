//! Server-owned facts consumed by protocol transitions.
//!
//! Everything here is a *binding-side fact*: secrets minted from the
//! operating system's entropy source, canonical injective digests of
//! non-secret request bodies, and monotonic clock reads. No lifecycle rule
//! lives here — the protocol crate decides what each fact means.

use liminal_protocol::wire::{DetachRequest, EnrollmentToken};

/// Fixed server-side fingerprint width shared by every lifecycle digest.
///
/// The four protocol fingerprint domains (enrollment, verifier, leave,
/// detach-cell) are instantiated with this one array type in the production
/// binding.
pub type Digest = [u8; 32];

/// Failure to mint a server-owned fact.
#[derive(Debug, thiserror::Error)]
pub enum FactsError {
    /// The operating system entropy source could not be read.
    #[error("failed to read entropy from {ENTROPY_SOURCE}: {0}")]
    Entropy(std::io::Error),
    /// The system clock reported a time before the Unix epoch or beyond the
    /// millisecond `u64` domain.
    #[error("system clock is outside the representable millisecond domain")]
    Clock,
}

/// Path of the operating-system entropy source used for secret minting.
const ENTROPY_SOURCE: &str = "/dev/urandom";

/// Mints 32 cryptographically random bytes for a new attach secret.
///
/// Reads the operating system's entropy device directly so the server takes
/// no new library dependency. A short read or unreadable device is a loud
/// typed failure — a predictable attach secret must never be issued.
///
/// # Errors
///
/// Returns [`FactsError::Entropy`] when the entropy source cannot provide
/// exactly 32 bytes.
pub fn mint_secret_bytes() -> Result<[u8; 32], FactsError> {
    use std::io::Read;
    let mut bytes = [0_u8; 32];
    let mut source = std::fs::File::open(ENTROPY_SOURCE).map_err(FactsError::Entropy)?;
    source.read_exact(&mut bytes).map_err(FactsError::Entropy)?;
    Ok(bytes)
}

/// Current wall-clock milliseconds since the Unix epoch.
///
/// # Errors
///
/// Returns [`FactsError::Clock`] when the clock precedes the epoch or the
/// millisecond count exceeds `u64` — receipt deadlines must never be derived
/// from a corrupted clock read.
pub fn now_unix_millis() -> Result<u64, FactsError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| FactsError::Clock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| FactsError::Clock)
}

/// Canonical injective enrollment-token fingerprint.
///
/// The 16 token bytes embed verbatim into the first half of the digest; the
/// second half is zero. The mapping is injective over the token domain, which
/// is all the permanent token→identity mapping requires — the token itself is
/// already stored durably by the transition log, so no hiding property is
/// claimed or needed.
#[must_use]
pub fn enrollment_fingerprint(token: EnrollmentToken) -> Digest {
    let mut digest = [0_u8; 32];
    let bytes = token.into_bytes();
    if let Some(prefix) = digest.get_mut(..bytes.len()) {
        prefix.copy_from_slice(&bytes);
    }
    digest
}

/// Canonical injective non-secret detach-request verifier.
///
/// Layout: detach attempt token (16 bytes) | capability generation (8 bytes,
/// big-endian) | participant id (8 bytes, big-endian). Conversation id is
/// fixed per stream and therefore omitted without losing injectivity within
/// one conversation's verifier domain.
#[must_use]
pub fn detach_request_verifier(request: &DetachRequest) -> Digest {
    let mut digest = [0_u8; 32];
    let token = request.detach_attempt_token.into_bytes();
    let generation = request.capability_generation.get().to_be_bytes();
    let participant = request.participant_id.to_be_bytes();
    for (slot, byte) in digest.iter_mut().zip(
        token
            .iter()
            .chain(generation.iter())
            .chain(participant.iter()),
    ) {
        *slot = *byte;
    }
    digest
}

/// Constant-time byte-slice equality for presented secrets.
///
/// Folds an XOR of every byte pair into one accumulator and never returns
/// early, so runtime depends only on input lengths — the same discipline as
/// the connection auth-token comparison.
#[must_use]
pub fn constant_time_eq(expected: &[u8], candidate: &[u8]) -> bool {
    let mut difference = u8::from(expected.len() != candidate.len());
    for (left, right) in expected.iter().zip(candidate.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}
