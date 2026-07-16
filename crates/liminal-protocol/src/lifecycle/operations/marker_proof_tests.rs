#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use crate::algebra::WideResourceVector;
use crate::wire::{
    AckNoOp, AttachAttemptToken, AttachMarkerProof, AttachSecret, BindingEpoch,
    ConnectionIncarnation, CredentialAttachRequest, Generation, MarkerAck, MarkerAckEnvelope,
    MarkerAckProof, MarkerMismatch, MarkerMismatchBody, MarkerNotDelivered,
    MarkerNotDeliveredReason, MarkerProofRequest,
};

use super::super::edge::{
    ClosureDebt, ClosureState, Event, ParticipantCursorProgress, StoredEdge,
    marker_delivery_for_test,
};
use super::marker_proof::{
    MarkerProofDecision, MarkerProofInput, MarkerProofState, select_marker_proof,
};

const CONVERSATION_ID: u64 = 34;
const PARTICIPANT_ID: u64 = 34;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn epoch(generation: u64, ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(34, ordinal),
        self::generation(generation),
    )
}

fn marker_ack(marker_delivery_seq: u64) -> MarkerAck {
    MarkerAck {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(7),
        marker_delivery_seq,
    }
}

fn attach(marker_delivery_seq: Option<u64>) -> CredentialAttachRequest {
    CredentialAttachRequest {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(7),
        attach_secret: AttachSecret::new([0x34; 32]),
        attach_attempt_token: AttachAttemptToken::new([0xA3; 16]),
        accept_marker_delivery_seq: marker_delivery_seq,
    }
}

fn ack_proof(marker_delivery_seq: u64) -> MarkerProofRequest {
    MarkerProofRequest::MarkerAck(MarkerAckProof {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(7),
        requested_marker_delivery_seq: marker_delivery_seq,
    })
}

fn proof_state(
    current_cursor: u64,
    accepted_marker_at_cursor: bool,
    expected_marker_delivery_seq: Option<u64>,
    progress: Option<ParticipantCursorProgress>,
) -> MarkerProofState {
    MarkerProofState::new(
        current_cursor,
        accepted_marker_at_cursor,
        expected_marker_delivery_seq,
        epoch(7, 1),
        progress,
    )
}

fn marker_progress(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
) -> ParticipantCursorProgress {
    let debt = ClosureDebt::new(WideResourceVector::new(1, 1))
        .expect("marker delivery fixture has nonzero closure debt");
    let delivery = marker_delivery_for_test(participant_id, binding_epoch, marker_delivery_seq)
        .expect("validated marker fixture restores");
    let state = delivery
        .delivered(
            debt,
            Event::marker_delivered(participant_id, binding_epoch, marker_delivery_seq),
        )
        .expect("exact final-emitter delivery commits");
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = state
    else {
        panic!("delivery must derive marker-backed cursor progress")
    };
    progress
}

#[test]
fn credential_attach_input_requires_some_marker_and_preserves_its_distinct_envelope() {
    assert_eq!(MarkerProofInput::credential_attach(&attach(None)), None);

    let input = MarkerProofInput::credential_attach(&attach(Some(20)))
        .expect("an explicit attach marker enters marker proof");
    assert_eq!(
        input,
        MarkerProofInput::CredentialAttach(AttachMarkerProof {
            conversation_id: CONVERSATION_ID,
            token: AttachAttemptToken::new([0xA3; 16]),
            participant_id: PARTICIPANT_ID,
            capability_generation: generation(7),
            requested_marker_delivery_seq: 20,
        })
    );

    let no_anchor = proof_state(10, false, None, None);
    assert_eq!(
        select_marker_proof(&no_anchor, input),
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: MarkerProofRequest::CredentialAttach(AttachMarkerProof {
                conversation_id: CONVERSATION_ID,
                token: AttachAttemptToken::new([0xA3; 16]),
                participant_id: PARTICIPANT_ID,
                capability_generation: generation(7),
                requested_marker_delivery_seq: 20,
            }),
            mismatch: MarkerMismatchBody::NoMarkerExpected,
        })
    );
}

#[test]
fn case_34_marker_ack_matrix_uses_the_frozen_total_precedence() {
    let anchored_undelivered = proof_state(10, false, Some(20), None);

    assert_eq!(
        select_marker_proof(
            &anchored_undelivered,
            MarkerProofInput::marker_ack(&marker_ack(9)),
        ),
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: ack_proof(9),
            mismatch: MarkerMismatchBody::BelowCursor { current_cursor: 10 },
        })
    );
    assert_eq!(
        select_marker_proof(
            &anchored_undelivered,
            MarkerProofInput::marker_ack(&marker_ack(10)),
        ),
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: ack_proof(10),
            mismatch: MarkerMismatchBody::ExpectedDifferentMarker {
                expected_marker_delivery_seq: 20,
            },
        }),
        "cursor equality is not a marker replay without an accepted marker record"
    );
    assert_eq!(
        select_marker_proof(
            &anchored_undelivered,
            MarkerProofInput::marker_ack(&marker_ack(19)),
        ),
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: ack_proof(19),
            mismatch: MarkerMismatchBody::ExpectedDifferentMarker {
                expected_marker_delivery_seq: 20,
            },
        })
    );
    assert_eq!(
        select_marker_proof(
            &anchored_undelivered,
            MarkerProofInput::marker_ack(&marker_ack(20)),
        ),
        MarkerProofDecision::MarkerNotDelivered(MarkerNotDelivered {
            request: ack_proof(20),
            reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            expected_marker_delivery_seq: 20,
        })
    );

    let no_anchor = proof_state(10, false, None, None);
    assert_eq!(
        select_marker_proof(&no_anchor, MarkerProofInput::marker_ack(&marker_ack(20)),),
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: ack_proof(20),
            mismatch: MarkerMismatchBody::NoMarkerExpected,
        })
    );

    let accepted = proof_state(
        20,
        true,
        None,
        Some(marker_progress(PARTICIPANT_ID, epoch(7, 1), 20)),
    );
    assert_eq!(
        select_marker_proof(&accepted, MarkerProofInput::marker_ack(&marker_ack(20)),),
        MarkerProofDecision::AckNoOp(AckNoOp::marker_ack(MarkerAckEnvelope {
            conversation_id: CONVERSATION_ID,
            participant_id: PARTICIPANT_ID,
            capability_generation: generation(7),
            marker_delivery_seq: 20,
        }))
    );
    assert_eq!(
        select_marker_proof(&accepted, MarkerProofInput::marker_ack(&marker_ack(19)),),
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: ack_proof(19),
            mismatch: MarkerMismatchBody::BelowCursor { current_cursor: 20 },
        })
    );

    let accepted_attach = MarkerProofInput::credential_attach(&attach(Some(20)))
        .expect("attach carries an explicit marker");
    assert!(matches!(
        select_marker_proof(&accepted, accepted_attach),
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: MarkerProofRequest::CredentialAttach(_),
            mismatch: MarkerMismatchBody::NoMarkerExpected,
        })
    ));
}

#[test]
fn exact_delivery_mints_an_opaque_operation_bound_permit() {
    let proof_epoch = epoch(7, 1);
    let progress = marker_progress(PARTICIPANT_ID, proof_epoch, 20);
    let state = proof_state(10, false, Some(20), Some(progress));

    let ack_input = MarkerProofInput::marker_ack(&marker_ack(20));
    let MarkerProofDecision::Permit(ack_permit) = select_marker_proof(&state, ack_input.clone())
    else {
        panic!("exact marker ack delivery must produce a permit")
    };
    assert_eq!(ack_permit.operation(), &ack_input);
    assert_eq!(ack_permit.expected_marker_delivery_seq(), 20);
    assert_eq!(ack_permit.proof_binding_epoch(), proof_epoch);
    assert_eq!(ack_permit.progress(), progress);

    let attach_input = MarkerProofInput::credential_attach(&attach(Some(20)))
        .expect("attach carries an explicit marker");
    let MarkerProofDecision::Permit(attach_permit) =
        select_marker_proof(&state, attach_input.clone())
    else {
        panic!("exact attach delivery must produce a permit")
    };
    assert_eq!(attach_permit.operation(), &attach_input);
    assert_ne!(attach_permit.operation(), ack_permit.operation());
    assert_eq!(attach_permit.progress(), progress);
}

#[test]
fn wrong_participant_epoch_marker_or_generation_never_counts_as_exact_delivery() {
    let request = MarkerProofInput::marker_ack(&marker_ack(20));
    let expected = MarkerProofDecision::MarkerNotDelivered(MarkerNotDelivered {
        request: ack_proof(20),
        reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
        expected_marker_delivery_seq: 20,
    });
    let wrong_progress = [
        marker_progress(PARTICIPANT_ID + 1, epoch(7, 1), 20),
        marker_progress(PARTICIPANT_ID, epoch(7, 2), 20),
        marker_progress(PARTICIPANT_ID, epoch(7, 1), 21),
    ];
    for progress in wrong_progress {
        let state = proof_state(10, false, Some(20), Some(progress));
        assert_eq!(select_marker_proof(&state, request.clone()), expected);
    }

    let continuous = ParticipantCursorProgress::continuous(PARTICIPANT_ID, epoch(7, 1), 20);
    assert_eq!(
        select_marker_proof(
            &proof_state(10, false, Some(20), Some(continuous)),
            request.clone(),
        ),
        expected,
        "continuous PCP has no delivered-marker provenance"
    );

    let wrong_proof_generation = MarkerProofState::new(
        10,
        false,
        Some(20),
        epoch(8, 1),
        Some(marker_progress(PARTICIPANT_ID, epoch(8, 1), 20)),
    );
    assert_eq!(
        select_marker_proof(&wrong_proof_generation, request),
        expected
    );
}

#[test]
fn selection_is_nonmutating_and_crash_replay_is_identical() {
    let state = proof_state(
        10,
        false,
        Some(20),
        Some(marker_progress(PARTICIPANT_ID, epoch(7, 1), 20)),
    );
    let before = state;
    let input = MarkerProofInput::marker_ack(&marker_ack(20));
    let first = select_marker_proof(&state, input.clone());
    let replay = select_marker_proof(&state, input);

    assert_eq!(state, before);
    assert_eq!(replay, first);
    assert!(matches!(first, MarkerProofDecision::Permit(_)));
}
