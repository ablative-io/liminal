//! Semantic connection-capacity production-path coverage.

use std::error::Error;

use liminal_protocol::wire::{
    ClientRequest, ConnectionConversationCapacityExceeded, ConnectionIncarnation,
    EnrollmentEnvelope, EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck,
    ResponseEnvelope, ServerValue,
};

use crate::server::participant::ParticipantConnectionConversations;

use super::super::ProductionParticipantHandler;
use super::super::tests::{
    dispatch, dispatch_tracked, open_disk_store_for_tests, test_participant_config,
};

/// Register row 5641: the first decoded semantic operation for an untracked
/// conversation on a connection whose semantic-conversation map is full
/// answers `ConnectionConversationCapacityExceeded` with the triggering
/// operation's exact common request envelope and the signed limit — while
/// already-tracked conversations keep operating and a different connection's
/// fresh map is unaffected.
#[test]
fn semantic_conversations_beyond_connection_limit_refuse_with_exact_envelope()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(85, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let mut config = test_participant_config();
    config.max_semantic_conversations_per_connection = 2;
    let handler = ProductionParticipantHandler::new(store, config)?;
    let mut conversations = ParticipantConnectionConversations::default();

    // Two enrollments fill the connection's two conversation slots.
    let mut secrets = Vec::new();
    for (index, conversation_id) in [901_u64, 902].into_iter().enumerate() {
        let enrolled = dispatch_tracked(
            &handler,
            incarnation,
            &mut conversations,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([100 + u8::try_from(index)?; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment {conversation_id} did not bind: {enrolled:?}").into());
        };
        secrets.push(receipt);
    }

    // Create a real recipient endpoint for the tracked-conversation probe on a
    // different connection map; participant zero's own enrollment is excluded.
    let peer = dispatch(
        &handler,
        ConnectionIncarnation::new(85, 3),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 901,
            enrollment_token: EnrollmentToken::new([109; 16]),
        }),
    )?;
    assert!(
        matches!(peer, ServerValue::EnrollBound(_)),
        "peer enrollment did not create a tracked obligation: {peer:?}"
    );

    // The third conversation's first semantic operation is the exact typed
    // refusal: the enrollment common request envelope plus the signed limit.
    let refused_token = [111; 16];
    let refused = dispatch_tracked(
        &handler,
        incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 903,
            enrollment_token: EnrollmentToken::new(refused_token),
        }),
    )?;
    let ServerValue::ConnectionConversationCapacityExceeded(
        ConnectionConversationCapacityExceeded::SemanticRequest {
            request:
                ResponseEnvelope::Enrollment(EnrollmentEnvelope {
                    conversation_id,
                    enrollment_token,
                }),
            limit,
        },
    ) = refused
    else {
        return Err(
            format!("third conversation must refuse on connection capacity: {refused:?}").into(),
        );
    };
    assert_eq!(conversation_id, 903);
    assert_eq!(enrollment_token, EnrollmentToken::new(refused_token));
    assert_eq!(limit, 2);

    // An ALREADY TRACKED conversation keeps operating at full capacity.
    let acked = dispatch_tracked(
        &handler,
        incarnation,
        &mut conversations,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: 901,
            participant_id: secrets
                .first()
                .ok_or("first enrollment receipt is present")?
                .participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 2,
        }),
    )?;
    assert!(
        matches!(acked, ServerValue::AckCommitted(_)),
        "a tracked conversation must keep operating at full capacity: {acked:?}"
    );

    // A DIFFERENT connection has its own fresh map: conversation 903 enrolls.
    let mut sibling_map = ParticipantConnectionConversations::default();
    let sibling = dispatch_tracked(
        &handler,
        ConnectionIncarnation::new(85, 2),
        &mut sibling_map,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 903,
            enrollment_token: EnrollmentToken::new(refused_token),
        }),
    )?;
    assert!(
        matches!(sibling, ServerValue::EnrollBound(_)),
        "a fresh connection map must admit the refused conversation: {sibling:?}"
    );
    Ok(())
}
