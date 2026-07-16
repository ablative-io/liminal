//! Refusal-residue production-path test (gap-closure fix round).
//!
//! Pins the contract's refusal law (R-C1: "every refusal commits no receipt,
//! order, cursor, binding, lifecycle record, candidate, or retention
//! mutation") against the live dispatch seam: refused probes of never-seen
//! conversation ids leave the durable store byte-identical and the in-memory
//! registry empty.

use std::error::Error;
use std::sync::Arc;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, Generation, ParticipantAck, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

/// Refused requests against never-seen conversation ids leave the durable
/// store byte-identical and the registry empty: no genesis is minted before
/// classification, and probe cells are evicted (R-C1 refusal law — "every
/// refusal commits no receipt, order, cursor, binding, lifecycle record,
/// candidate, or retention mutation").
#[test]
fn refused_probes_of_fresh_conversations_leave_no_durable_or_registry_residue()
-> Result<(), Box<dyn Error>> {
    use liminal::durability::bridge::block_on;
    use liminal_protocol::wire::{LeaveAttemptToken, LeaveRequest, MarkerAck};

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(51, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config());

    // One refused probe per pure-refusal arm, each on its own fresh id.
    let acked = dispatch(
        &handler,
        incarnation,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: 9001,
            participant_id: 0,
            capability_generation: Generation::ONE,
            through_seq: 1,
        }),
    )?;
    assert!(
        matches!(acked, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation ack must refuse ParticipantUnknown: {acked:?}"
    );
    let detached = dispatch(
        &handler,
        incarnation,
        ClientRequest::Detach(DetachRequest {
            conversation_id: 9002,
            participant_id: 0,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([9; 16]),
        }),
    )?;
    assert!(
        matches!(detached, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation detach must refuse ParticipantUnknown: {detached:?}"
    );
    let left = dispatch(
        &handler,
        incarnation,
        ClientRequest::Leave(LeaveRequest {
            conversation_id: 9003,
            participant_id: 0,
            capability_generation: Generation::ONE,
            attach_secret: liminal_protocol::wire::AttachSecret::new([3; 32]),
            leave_attempt_token: LeaveAttemptToken::new([4; 16]),
        }),
    )?;
    assert!(
        matches!(left, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation leave must refuse ParticipantUnknown: {left:?}"
    );
    let marker = dispatch(
        &handler,
        incarnation,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id: 9004,
            participant_id: 0,
            capability_generation: Generation::ONE,
            marker_delivery_seq: 1,
        }),
    )?;
    assert!(
        matches!(marker, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation marker ack must refuse ParticipantUnknown: {marker:?}"
    );
    let attach = dispatch(
        &handler,
        incarnation,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: 9005,
            participant_id: 0,
            capability_generation: Generation::ONE,
            attach_secret: liminal_protocol::wire::AttachSecret::new([5; 32]),
            attach_attempt_token: AttachAttemptToken::new([6; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(
        matches!(attach, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation attach must refuse ParticipantUnknown: {attach:?}"
    );

    // Durable store: every probed conversation stream is byte-absent, and no
    // observer row exists.
    for conversation_id in [9001_u64, 9002, 9003, 9004, 9005] {
        let stream_key = format!("liminal:participant-production:{conversation_id}");
        let entries = block_on(store.read_from(&stream_key, 0, 8))??;
        assert!(
            entries.is_empty(),
            "refused probe minted durable entries for conversation {conversation_id}"
        );
    }
    let observer_rows = block_on(store.read_from("liminal:participant-observer-recovery", 0, 8))??;
    assert!(
        observer_rows.is_empty(),
        "refused probes minted observer rows: {observer_rows:?}"
    );
    // Registry: every probe cell was evicted.
    assert_eq!(
        handler.registry_len(),
        0,
        "refused probes left in-memory registry cells behind"
    );
    Ok(())
}
