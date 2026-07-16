//! Request-bound response authority.
//!
//! Every decision arm that answers a specific [`super::ClientRequest`] carries
//! one of these bound types instead of a bare [`super::ServerValue`]. Each
//! bound type exposes constructors ONLY for the server values the frozen
//! contract's R-D1 register admits for its request, so pairing a request with
//! an outcome outside its legal set — for example answering a
//! `RecordAdmission` with `EnrollBound` — is a compile error by construction,
//! the same discipline as the four-variant detach cell.
//!
//! The legal request-to-response matrix is transcribed, not invented: it is
//! the R-D1 register of `docs/design/PARTICIPANT-CONTRACT.md` @
//! `55856ae3c53206f9c662e6815650dfc67a89ce85` (the outcome table at lines
//! 5624-5689 and the exhaustive-pair rule at lines 5773-5784). Constructor
//! doc comments cite the exact register rows.
//!
//! Constructor visibility follows one auditable rule:
//!
//! * A constructor whose arguments cannot encode another request's origin —
//!   the request's own common envelope plus response-specific suffix fields,
//!   or a payload type that exists only for this request — is `pub`.
//! * A constructor accepting a multi-request union payload (for example
//!   [`super::Retired`] or [`super::ObserverBackpressure`]) is `pub(crate)`:
//!   those values are minted exclusively by this crate's own selectors for
//!   the exact request flow that invokes them, and a consuming server can
//!   never hand-build a refusal from them.
//!
//! Exactly one wire value stays unbound: `ParticipantTransportRejected`
//! (`0x0100`) is presemantic — the register row at line 5626 and the routing
//! rule at lines 5779-5781 state it has no decodable originating request —
//! so it is deliberately absent from every bound type here.

mod acks;
mod credential_attach;
mod enrollment;
mod lifecycle;
mod records;

pub use acks::{MarkerAckResponse, ParticipantAckResponse};
pub use credential_attach::CredentialAttachResponse;
pub use enrollment::EnrollmentResponse;
pub use lifecycle::{DetachResponse, LeaveResponse};
pub use records::{ObserverRecoveryResponse, RecordAdmissionResponse};
