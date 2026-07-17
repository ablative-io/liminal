use alloc::string::ToString;

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
