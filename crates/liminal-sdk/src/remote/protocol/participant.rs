use alloc::string::ToString;
use core::time::Duration;

use liminal_protocol::wire::{ClientRequest, ParticipantFrame};

use crate::SdkError;

use super::super::ServerAddress;
use super::super::participant::ParticipantResponseProvenance;
use super::ProtocolRemoteTransport;

#[derive(Debug)]
pub struct ParticipantTransportFrame {
    pub(in crate::remote) frame: ParticipantFrame,
    pub(in crate::remote) provenance: ParticipantResponseProvenance,
}

pub trait ParticipantRemoteTransport {
    fn send_participant(
        &self,
        server_address: &ServerAddress,
        request: &ClientRequest,
    ) -> Result<ParticipantResponseProvenance, SdkError>;

    fn receive_participant(
        &self,
        server_address: &ServerAddress,
    ) -> Result<ParticipantTransportFrame, SdkError>;

    /// Reads one participant frame if one arrives within `budget`, reporting a
    /// quiet window as `Ok(None)` rather than as a failure.
    ///
    /// Deliberately carries NO default implementation. The obvious default —
    /// delegate to [`receive_participant`](Self::receive_participant) and wrap
    /// the result in `Some` — would answer a quiet connection by waiting out the
    /// full response deadline while presenting itself as bounded, which is the
    /// exact dishonesty this method exists to remove, and it would do so
    /// silently for any transport that forgot to override it. Requiring every
    /// mount to answer is what makes a second implementation obviously complete
    /// rather than plausibly complete.
    ///
    /// # Errors
    /// Any transport read or decode failure. A quiet window is NOT one.
    fn receive_participant_within(
        &self,
        server_address: &ServerAddress,
        budget: Duration,
    ) -> Result<Option<ParticipantTransportFrame>, SdkError>;

    fn reconnect_participant(
        &self,
        server_address: &ServerAddress,
    ) -> Result<ParticipantResponseProvenance, SdkError>;
}

impl ParticipantRemoteTransport for ProtocolRemoteTransport {
    fn send_participant(
        &self,
        server_address: &ServerAddress,
        request: &ClientRequest,
    ) -> Result<ParticipantResponseProvenance, SdkError> {
        core::hint::black_box((server_address, request));
        Err(SdkError::Protocol {
            description: "participant operations require a connected real transport".to_string(),
        })
    }

    fn receive_participant(
        &self,
        server_address: &ServerAddress,
    ) -> Result<ParticipantTransportFrame, SdkError> {
        core::hint::black_box(server_address);
        Err(SdkError::Protocol {
            description: "participant receive requires a connected real transport".to_string(),
        })
    }

    /// Refuses rather than reporting quiet: an unmounted transport has no
    /// connection to be quiet ON, and `Ok(None)` here would tell a drain loop
    /// its backlog was empty when it never had one.
    fn receive_participant_within(
        &self,
        server_address: &ServerAddress,
        budget: Duration,
    ) -> Result<Option<ParticipantTransportFrame>, SdkError> {
        core::hint::black_box((server_address, budget));
        Err(SdkError::Protocol {
            description: "participant receive requires a connected real transport".to_string(),
        })
    }

    fn reconnect_participant(
        &self,
        server_address: &ServerAddress,
    ) -> Result<ParticipantResponseProvenance, SdkError> {
        core::hint::black_box(server_address);
        Err(SdkError::Protocol {
            description: "participant reconnect requires a connected real transport".to_string(),
        })
    }
}
