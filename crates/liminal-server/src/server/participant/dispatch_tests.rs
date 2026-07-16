use std::sync::Mutex;

use liminal::protocol::{Frame, decode as decode_generic};
use liminal_protocol::{
    lifecycle::{
        BindingRequiredLookupResult, BindingState, ParticipantBindingRequest, PresentedIdentity,
        lookup_binding_required,
    },
    wire::{
        AttachAttemptToken, AttachSecret, ClientRequest, ConnectionIncarnation,
        CredentialAttachRequest, DetachAttemptToken, DetachRequest, EnrollmentRequest,
        EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, MarkerAck,
        ObserverRecoveryHandshake, ObserverRefusal, ParticipantAck, ParticipantFrame,
        ParticipantUnknown, RecordAdmission, ServerValue, decode, encode, encoded_len,
    },
};

use super::{
    dispatch::{
        ParticipantConnectionContext, ParticipantDispatch, ParticipantDispatchError,
        ParticipantSemanticError, ParticipantSemanticHandler, dispatch_generic_frame,
    },
    transport::ParticipantSession,
};

#[derive(Debug)]
struct RecordingHandler {
    seen: Mutex<Vec<(ParticipantConnectionContext, ClientRequest)>>,
    fail: bool,
}

impl RecordingHandler {
    fn successful() -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
            fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
            fail: true,
        }
    }

    fn calls(&self) -> Result<usize, String> {
        self.seen
            .lock()
            .map(|seen| seen.len())
            .map_err(|_| "recording handler mutex poisoned".to_owned())
    }
}

impl ParticipantSemanticHandler for RecordingHandler {
    fn handle(
        &self,
        context: ParticipantConnectionContext,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        self.seen
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "recording handler mutex poisoned".to_owned(),
            })?
            .push((context, request));
        if self.fail {
            return Err(ParticipantSemanticError::Unavailable);
        }
        let lookup = lookup_binding_required::<[u8; 1], [u8; 1], [u8; 1]>(
            PresentedIdentity::Absent,
            &BindingState::Detached,
            None,
            &ParticipantBindingRequest::RecordAdmission(RecordAdmission {
                conversation_id: 99,
                participant_id: 1,
                capability_generation: Generation::ONE,
                payload: Vec::new(),
            }),
        );
        let BindingRequiredLookupResult::ParticipantUnknown(value) = lookup else {
            return Err(ParticipantSemanticError::Internal {
                message: "protocol fixture did not select ParticipantUnknown".to_owned(),
            });
        };
        Ok(ServerValue::ParticipantUnknown(value))
    }
}

fn participant_generic(request: ClientRequest) -> Result<Frame, String> {
    let participant = ParticipantFrame::ClientRequest(request);
    let mut bytes = vec![0; encoded_len(&participant).map_err(|error| format!("{error:?}"))?];
    let written = encode(&participant, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    let (generic, consumed) = decode_generic(&bytes).map_err(|error| error.to_string())?;
    if consumed != bytes.len() {
        return Err("generic decoder left an unread suffix".to_owned());
    }
    Ok(generic)
}

fn negotiated_session() -> Result<ParticipantSession, String> {
    let mut session = ParticipantSession::default();
    session
        .negotiate_v1()
        .map_err(|error| format!("{error:?}"))?;
    Ok(session)
}

fn context() -> ParticipantConnectionContext {
    ParticipantConnectionContext::new(ConnectionIncarnation::new(4, 9))
}

#[test]
fn decoded_request_reaches_handler_and_crate_value_is_framed() -> Result<(), String> {
    let request = ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: 70,
        participant_id: 2,
        capability_generation: Generation::new(3)
            .ok_or_else(|| "fixture generation was zero".to_owned())?,
        payload: vec![1, 2, 3],
    });
    let generic = participant_generic(request.clone())?;
    let handler = RecordingHandler::successful();

    let ParticipantDispatch::Respond(response) =
        dispatch_generic_frame(&generic, true, negotiated_session()?, context(), &handler)
    else {
        return Err("semantic request did not produce a response".to_owned());
    };
    assert_eq!(handler.calls()?, 1);
    let generic_len =
        liminal::protocol::encoded_len(&response).map_err(|error| error.to_string())?;
    let mut response_bytes = vec![0; generic_len];
    let written = liminal::protocol::encode(&response, &mut response_bytes)
        .map_err(|error| error.to_string())?;
    response_bytes.truncate(written);
    let decoded = decode(
        &response_bytes,
        liminal_protocol::wire::ReceiverDirection::Client,
    )
    .map_err(|error| format!("{error:?}"))?;
    assert!(matches!(
        decoded,
        ParticipantFrame::ServerValue(ServerValue::ParticipantUnknown(ParticipantUnknown { .. }))
    ));
    let seen = handler
        .seen
        .lock()
        .map_err(|_| "recording handler mutex poisoned".to_owned())?;
    assert_eq!(seen.as_slice(), &[(context(), request)]);
    drop(seen);
    Ok(())
}

#[test]
fn gate_rejection_never_calls_semantics() -> Result<(), String> {
    let generic = participant_generic(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 71,
        enrollment_token: EnrollmentToken::new([7; 16]),
    }))?;
    let handler = RecordingHandler::successful();

    assert!(matches!(
        dispatch_generic_frame(&generic, false, negotiated_session()?, context(), &handler,),
        ParticipantDispatch::RespondThenClose(_)
    ));
    assert_eq!(handler.calls()?, 0);
    Ok(())
}

#[test]
fn semantic_failure_is_fatal_and_has_no_wire_value() -> Result<(), String> {
    let generic = participant_generic(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: 72,
        participant_id: 0,
        capability_generation: Generation::ONE,
        payload: vec![],
    }))?;
    let handler = RecordingHandler::failing();

    assert!(matches!(
        dispatch_generic_frame(&generic, true, negotiated_session()?, context(), &handler,),
        ParticipantDispatch::Fatal(ParticipantDispatchError::Semantic(
            ParticipantSemanticError::Unavailable
        ))
    ));
    assert_eq!(handler.calls()?, 1);
    Ok(())
}

#[test]
fn unrelated_generic_frame_bypasses_handler() -> Result<(), String> {
    let handler = RecordingHandler::successful();
    let generic = Frame::Unknown {
        type_id: 0xEE,
        flags: 0,
        stream_id: 4,
        payload: vec![9],
    };
    assert!(matches!(
        dispatch_generic_frame(
            &generic,
            true,
            ParticipantSession::default(),
            context(),
            &handler,
        ),
        ParticipantDispatch::NotParticipant
    ));
    assert_eq!(handler.calls()?, 0);
    Ok(())
}

#[test]
fn every_registered_client_request_reaches_the_same_semantic_seam() -> Result<(), String> {
    let generation = Generation::new(7).ok_or_else(|| "fixture generation was zero".to_owned())?;
    let requests = vec![
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 1,
            enrollment_token: EnrollmentToken::new([1; 16]),
        }),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: 2,
            participant_id: 3,
            capability_generation: generation,
            attach_secret: AttachSecret::new([2; 32]),
            attach_attempt_token: AttachAttemptToken::new([3; 16]),
            accept_marker_delivery_seq: Some(4),
        }),
        ClientRequest::Detach(DetachRequest {
            conversation_id: 5,
            participant_id: 6,
            capability_generation: generation,
            detach_attempt_token: DetachAttemptToken::new([4; 16]),
        }),
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: 7,
            participant_id: 8,
            capability_generation: generation,
            through_seq: 9,
        }),
        ClientRequest::Leave(LeaveRequest {
            conversation_id: 10,
            participant_id: 11,
            capability_generation: generation,
            attach_secret: AttachSecret::new([5; 32]),
            leave_attempt_token: LeaveAttemptToken::new([6; 16]),
        }),
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id: 12,
            participant_id: 13,
            capability_generation: generation,
            marker_delivery_seq: 14,
        }),
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: 15,
            participant_id: 16,
            capability_generation: generation,
            payload: vec![0xAA, 0xBB, 0xCC],
        }),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![
                ObserverRefusal {
                    conversation_id: 17,
                    refused_epoch: 18,
                },
                ObserverRefusal {
                    conversation_id: 19,
                    refused_epoch: 20,
                },
            ],
        }),
    ];
    let handler = RecordingHandler::failing();

    for request in &requests {
        let generic = participant_generic(request.clone())?;
        assert!(matches!(
            dispatch_generic_frame(&generic, true, negotiated_session()?, context(), &handler,),
            ParticipantDispatch::Fatal(ParticipantDispatchError::Semantic(
                ParticipantSemanticError::Unavailable
            ))
        ));
    }

    let seen = handler
        .seen
        .lock()
        .map_err(|_| "recording handler mutex poisoned".to_owned())?;
    let expected: Vec<_> = requests
        .into_iter()
        .map(|request| (context(), request))
        .collect();
    assert_eq!(*seen, expected);
    drop(seen);
    Ok(())
}
