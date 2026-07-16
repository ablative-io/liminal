#![allow(clippy::expect_used, clippy::panic)]

use alloc::vec;
use alloc::vec::Vec;

use proptest::prelude::*;

use crate::wire::{
    BindingEpoch, ConnectionIncarnation, DetachAttemptToken, Generation, LeaveAttemptToken,
    LeaveCommitted,
};

use super::operation_event::{
    AttachedOperation, BindingFateOperation, ConversationOperation, DetachedOperation,
    EnrolledOperation, LeftOperation, NonzeroDebtAckOperation,
};
use super::{
    ConversationCommit, ConversationDecision, ConversationEvent, ConversationEventDecodeError,
    ConversationGenesis, ConversationRefusalReason, ConversationReplayError,
    ParticipantConversation,
};

const CONVERSATION_ID: u64 = 91;

fn pending_genesis(conversation_id: u64) -> ConversationCommit {
    let conversation =
        ParticipantConversation::from_genesis(ConversationGenesis::new(conversation_id));
    let ConversationDecision::Commit(commit) = conversation.decide_genesis_validation() else {
        unreachable!("fresh genesis must select its one-shot event");
    };
    commit
}

#[test]
fn decision_holds_state_until_commit_and_encodes_stable_header() {
    let commit = pending_genesis(CONVERSATION_ID);
    assert_eq!(commit.event().conversation_id(), CONVERSATION_ID);
    assert_eq!(commit.event().ordinal(), 0);
    assert_eq!(commit.event().encoded_len(), 30);
    assert_eq!(
        commit.event().encode_canonical(),
        vec![
            b'L', b'P', b'C', b'E', 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 91, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 0,
        ]
    );

    let committed = commit.commit();
    assert!(committed.genesis_validated());
    assert_eq!(committed.next_event_ordinal(), 1);
}

#[test]
fn abort_recovers_the_exact_unmodified_prestate() {
    let aborted = pending_genesis(CONVERSATION_ID).abort();
    assert_eq!(aborted.conversation_id(), CONVERSATION_ID);
    assert_eq!(aborted.next_event_ordinal(), 0);
    assert!(!aborted.genesis_validated());

    assert!(matches!(
        aborted.decide_genesis_validation(),
        ConversationDecision::Commit(_)
    ));
}

#[test]
fn canonical_decode_rejects_truncation_unknown_codec_kind_and_length() {
    let canonical = pending_genesis(CONVERSATION_ID).event().encode_canonical();
    assert_eq!(
        ConversationEvent::decode_canonical(&canonical[..29]),
        Err(ConversationEventDecodeError::Truncated {
            required: 30,
            available: 29,
        })
    );

    let mut unknown_codec = canonical.clone();
    unknown_codec[5] = 2;
    assert_eq!(
        ConversationEvent::decode_canonical(&unknown_codec),
        Err(ConversationEventDecodeError::UnsupportedCodec { major: 2, minor: 0 })
    );

    let mut unknown_kind = canonical.clone();
    unknown_kind[25] = 8;
    assert_eq!(
        ConversationEvent::decode_canonical(&unknown_kind),
        Err(ConversationEventDecodeError::UnknownEventKind { tag: 8 })
    );

    let mut trailing = canonical;
    trailing.push(0);
    assert_eq!(
        ConversationEvent::decode_canonical(&trailing),
        Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len: 0,
            actual_body_len: 1,
        })
    );
}

#[test]
fn replay_requires_exact_conversation_ordinal_and_body_precondition() {
    let encoded = pending_genesis(CONVERSATION_ID).event().encode_canonical();
    let wrong_conversation = ParticipantConversation::from_genesis(ConversationGenesis::new(92));
    let failure = wrong_conversation
        .replay(ConversationEvent::decode_canonical(&encoded).expect("canonical event decodes"))
        .expect_err("another conversation must be rejected");
    assert_eq!(
        failure.reason(),
        ConversationReplayError::ConversationMismatch {
            expected: 92,
            actual: CONVERSATION_ID,
        }
    );
    assert_eq!(failure.into_conversation().next_event_ordinal(), 0);

    let committed = pending_genesis(CONVERSATION_ID).commit();
    let failure = committed
        .replay(ConversationEvent::decode_canonical(&encoded).expect("canonical event decodes"))
        .expect_err("replayed ordinal zero must not follow committed ordinal zero");
    assert_eq!(
        failure.reason(),
        ConversationReplayError::OrdinalMismatch {
            expected: 1,
            actual: 0,
        }
    );
    let committed = failure.into_conversation();
    assert!(committed.genesis_validated());
    assert_eq!(committed.next_event_ordinal(), 1);

    let committed = pending_genesis(CONVERSATION_ID).commit();
    let mut next_ordinal_genesis = encoded;
    next_ordinal_genesis[23] = 1;
    let failure = committed
        .replay(
            ConversationEvent::decode_canonical(&next_ordinal_genesis)
                .expect("syntactically valid next-ordinal event decodes"),
        )
        .expect_err("genesis body is one-shot");
    assert_eq!(
        failure.reason(),
        ConversationReplayError::GenesisAlreadyValidated
    );
    let committed = failure.into_conversation();
    assert!(committed.genesis_validated());
    assert_eq!(committed.next_event_ordinal(), 1);
}

#[test]
fn ordinal_exhaustion_refuses_before_commit_and_replay_mutation() {
    let genesis = ConversationGenesis::new(CONVERSATION_ID);
    let exhausted = ParticipantConversation::from_test_state(genesis, u64::MAX, false);
    let ConversationDecision::Refused(refusal) = exhausted.decide_genesis_validation() else {
        unreachable!("an unrepresentable successor cannot emit a commit");
    };
    assert_eq!(
        refusal.reason(),
        ConversationRefusalReason::EventOrdinalExhausted { ordinal: u64::MAX }
    );
    let exhausted = refusal.into_conversation();
    assert_eq!(exhausted.next_event_ordinal(), u64::MAX);
    assert!(!exhausted.genesis_validated());

    let mut encoded = pending_genesis(CONVERSATION_ID).event().encode_canonical();
    encoded[16..24].copy_from_slice(&u64::MAX.to_be_bytes());
    let failure = exhausted
        .replay(
            ConversationEvent::decode_canonical(&encoded)
                .expect("maximum-ordinal event is structurally valid"),
        )
        .expect_err("maximum ordinal has no post-event state");
    assert_eq!(
        failure.reason(),
        ConversationReplayError::EventOrdinalExhausted { ordinal: u64::MAX }
    );
    let unchanged = failure.into_conversation();
    assert_eq!(unchanged.next_event_ordinal(), u64::MAX);
    assert!(!unchanged.genesis_validated());
}

#[test]
fn replay_is_deterministic_and_matches_commit_consumption() {
    let commit = pending_genesis(CONVERSATION_ID);
    let encoded = commit.event().encode_canonical();
    let committed = commit.commit();

    let replay = || {
        ParticipantConversation::from_genesis(ConversationGenesis::new(CONVERSATION_ID))
            .replay(ConversationEvent::decode_canonical(&encoded).expect("canonical event decodes"))
    };
    let replayed_once = replay().expect("exact prestate replays");
    let replayed_twice = replay().expect("same prestate replays identically");
    assert_eq!(replayed_once, committed);
    assert_eq!(replayed_twice, committed);
    assert_eq!(
        ConversationEvent::decode_canonical(&encoded)
            .expect("canonical event decodes")
            .encode_canonical(),
        encoded
    );
}

#[test]
fn repeated_genesis_decision_refuses_and_preserves_committed_state() {
    let committed = pending_genesis(CONVERSATION_ID).commit();
    let ConversationDecision::Refused(refusal) = committed.decide_genesis_validation() else {
        unreachable!("genesis validation is one-shot");
    };
    assert_eq!(
        refusal.reason(),
        ConversationRefusalReason::GenesisAlreadyValidated
    );
    let recovered = refusal.into_conversation();
    assert!(recovered.genesis_validated());
    assert_eq!(recovered.next_event_ordinal(), 1);
}

fn validated_shell() -> ParticipantConversation {
    pending_genesis(CONVERSATION_ID).commit()
}

fn epoch(generation: u64, connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(3, connection_ordinal),
        Generation::new(generation).expect("test generations are nonzero"),
    )
}

fn enrolled_operation() -> ConversationOperation {
    ConversationOperation::Enrolled(
        EnrolledOperation::from_decoded(CONVERSATION_ID, 7, epoch(1, 4), 11, 12)
            .expect("generation-one enrollment body is canonical"),
    )
}

fn attached_operation() -> ConversationOperation {
    ConversationOperation::Attached(AttachedOperation::from_decoded(
        CONVERSATION_ID,
        7,
        epoch(2, 5),
        13,
        14,
    ))
}

fn detached_operation() -> ConversationOperation {
    ConversationOperation::Detached(DetachedOperation::from_decoded(
        DetachAttemptToken::new([0xD7; 16]),
        CONVERSATION_ID,
        7,
        epoch(2, 5),
        15,
        16,
    ))
}

fn left_operation(
    ended_binding_epoch: Option<BindingEpoch>,
    prior_terminal_delivery_seq: Option<u64>,
) -> ConversationOperation {
    let committed = LeaveCommitted::new(
        CONVERSATION_ID,
        LeaveAttemptToken::new([0xB7; 16]),
        7,
        Generation::new(4).expect("retired generation is nonzero"),
        ended_binding_epoch,
        prior_terminal_delivery_seq,
        31,
    )
    .expect("leave fixture satisfies the canonical result invariants");
    ConversationOperation::Left(LeftOperation::new(committed, 17))
}

fn binding_fate_operation() -> ConversationOperation {
    ConversationOperation::BindingFate(BindingFateOperation::from_decoded(7, epoch(3, 6), 21))
}

fn nonzero_debt_ack_operation() -> ConversationOperation {
    ConversationOperation::NonzeroDebtAck(NonzeroDebtAckOperation::from_decoded(
        CONVERSATION_ID,
        7,
        Generation::new(3).expect("ack generation is nonzero"),
        22,
    ))
}

fn six_operations() -> Vec<ConversationOperation> {
    vec![
        enrolled_operation(),
        attached_operation(),
        detached_operation(),
        left_operation(Some(epoch(4, 9)), Some(30)),
        binding_fate_operation(),
        nonzero_debt_ack_operation(),
    ]
}

fn assert_operation_round_trip(operation: ConversationOperation) {
    let ConversationDecision::Commit(commit) = validated_shell().decide_operation(operation) else {
        unreachable!("a validated shell accepts every same-conversation operation");
    };
    let encoded = commit.event().encode_canonical();
    assert_eq!(commit.event().encoded_len(), encoded.len());
    let decoded =
        ConversationEvent::decode_canonical(&encoded).expect("canonical operation event decodes");
    assert_eq!(&decoded, commit.event());
    assert_eq!(decoded.encode_canonical(), encoded);

    let committed = commit.commit();
    assert_eq!(committed.next_event_ordinal(), 2);
    assert!(committed.genesis_validated());

    let replayed = validated_shell()
        .replay(decoded)
        .expect("the exact committed pre-state replays the decoded event");
    assert_eq!(replayed, committed);
}

#[test]
fn every_lifecycle_operation_round_trips_encode_decode_replay() {
    for operation in six_operations() {
        assert_operation_round_trip(operation);
    }
}

#[test]
fn left_bodies_round_trip_every_optional_field_combination() {
    assert_operation_round_trip(left_operation(None, None));
    assert_operation_round_trip(left_operation(Some(epoch(4, 9)), None));
    assert_operation_round_trip(left_operation(None, Some(30)));
    assert_operation_round_trip(left_operation(Some(epoch(4, 9)), Some(30)));
}

#[test]
fn operation_decision_requires_genesis_validation_first() {
    let fresh = ParticipantConversation::from_genesis(ConversationGenesis::new(CONVERSATION_ID));
    let ConversationDecision::Refused(refusal) = fresh.decide_operation(attached_operation())
    else {
        unreachable!("an unvalidated shell must refuse lifecycle operations");
    };
    assert_eq!(
        refusal.reason(),
        ConversationRefusalReason::GenesisNotValidated
    );
    let fresh = refusal.into_conversation();
    assert_eq!(fresh.next_event_ordinal(), 0);
    assert!(!fresh.genesis_validated());
}

#[test]
fn operation_replay_requires_genesis_validation_first() {
    let ConversationDecision::Commit(commit) =
        validated_shell().decide_operation(attached_operation())
    else {
        unreachable!("a validated shell accepts the attach operation");
    };
    let mut encoded = commit.event().encode_canonical();
    encoded[16..24].copy_from_slice(&0_u64.to_be_bytes());
    let decoded = ConversationEvent::decode_canonical(&encoded)
        .expect("ordinal-zero operation event is structurally canonical");
    let fresh = ParticipantConversation::from_genesis(ConversationGenesis::new(CONVERSATION_ID));
    let failure = fresh
        .replay(decoded)
        .expect_err("an operation event must not precede genesis validation");
    assert_eq!(
        failure.reason(),
        ConversationReplayError::GenesisNotValidated
    );
    let fresh = failure.into_conversation();
    assert_eq!(fresh.next_event_ordinal(), 0);
    assert!(!fresh.genesis_validated());
}

#[test]
fn operation_decision_refuses_foreign_conversation_provenance() {
    let foreign = ConversationOperation::Attached(AttachedOperation::from_decoded(
        CONVERSATION_ID + 1,
        7,
        epoch(2, 5),
        13,
        14,
    ));
    let ConversationDecision::Refused(refusal) = validated_shell().decide_operation(foreign) else {
        unreachable!("provenance for another conversation must be refused");
    };
    assert_eq!(
        refusal.reason(),
        ConversationRefusalReason::OperationConversationMismatch {
            expected: CONVERSATION_ID,
            actual: CONVERSATION_ID + 1,
        }
    );
    let shell = refusal.into_conversation();
    assert_eq!(shell.next_event_ordinal(), 1);
    assert!(shell.genesis_validated());
}

#[test]
fn operation_decision_refuses_ordinal_exhaustion() {
    let genesis = ConversationGenesis::new(CONVERSATION_ID);
    let exhausted = ParticipantConversation::from_test_state(genesis, u64::MAX, true);
    let ConversationDecision::Refused(refusal) = exhausted.decide_operation(attached_operation())
    else {
        unreachable!("an unrepresentable successor cannot emit a commit");
    };
    assert_eq!(
        refusal.reason(),
        ConversationRefusalReason::EventOrdinalExhausted { ordinal: u64::MAX }
    );
}

#[test]
fn operation_abort_recovers_the_exact_unmodified_prestate() {
    let ConversationDecision::Commit(commit) =
        validated_shell().decide_operation(detached_operation())
    else {
        unreachable!("a validated shell accepts the detach operation");
    };
    let aborted = commit.abort();
    assert_eq!(aborted, validated_shell());
}

fn encoded_operation(operation: ConversationOperation) -> Vec<u8> {
    let ConversationDecision::Commit(commit) = validated_shell().decide_operation(operation) else {
        unreachable!("a validated shell accepts every same-conversation operation");
    };
    commit.event().encode_canonical()
}

#[test]
fn operation_decode_rejects_zero_generations_and_non_genesis_enrollment() {
    let mut zero_generation = encoded_operation(attached_operation());
    zero_generation[54..62].copy_from_slice(&0_u64.to_be_bytes());
    assert_eq!(
        ConversationEvent::decode_canonical(&zero_generation),
        Err(ConversationEventDecodeError::NonCanonicalBody { tag: 3 })
    );

    let mut non_genesis_enrollment = encoded_operation(enrolled_operation());
    non_genesis_enrollment[54..62].copy_from_slice(&2_u64.to_be_bytes());
    assert_eq!(
        ConversationEvent::decode_canonical(&non_genesis_enrollment),
        Err(ConversationEventDecodeError::NonCanonicalBody { tag: 2 })
    );

    let mut zero_ack_generation = encoded_operation(nonzero_debt_ack_operation());
    zero_ack_generation[38..46].copy_from_slice(&0_u64.to_be_bytes());
    assert_eq!(
        ConversationEvent::decode_canonical(&zero_ack_generation),
        Err(ConversationEventDecodeError::NonCanonicalBody { tag: 7 })
    );
}

#[test]
fn operation_decode_rejects_non_canonical_lengths() {
    let mut truncated = encoded_operation(attached_operation());
    truncated.pop();
    let Err(ConversationEventDecodeError::NonCanonicalLength { .. }) =
        ConversationEvent::decode_canonical(&truncated)
    else {
        panic!("a truncated attach body must fail as non-canonical");
    };

    let mut trailing = encoded_operation(binding_fate_operation());
    trailing.push(0);
    let Err(ConversationEventDecodeError::NonCanonicalLength { .. }) =
        ConversationEvent::decode_canonical(&trailing)
    else {
        panic!("a padded fate body must fail as non-canonical");
    };
}

#[test]
fn left_decode_rejects_unassigned_flags_and_invalid_results() {
    // Body layout after the 30-byte header: token(16) + participant(8) +
    // retired_generation(8) + flags(1) at absolute offset 62.
    let mut junk_flags = encoded_operation(left_operation(None, None));
    junk_flags[62] = 0b100;
    assert_eq!(
        ConversationEvent::decode_canonical(&junk_flags),
        Err(ConversationEventDecodeError::NonCanonicalBody { tag: 5 })
    );

    // Claim the ended-epoch flag without supplying its bytes.
    let mut missing_epoch = encoded_operation(left_operation(None, None));
    missing_epoch[62] = 0b01;
    let Err(ConversationEventDecodeError::NonCanonicalLength { .. }) =
        ConversationEvent::decode_canonical(&missing_epoch)
    else {
        panic!("a flagged-but-absent epoch must fail as non-canonical");
    };

    // A prior terminal at or after Left violates the permanent result.
    let mut inverted = encoded_operation(left_operation(None, Some(30)));
    let prior_at = 63;
    inverted[prior_at..prior_at + 8].copy_from_slice(&31_u64.to_be_bytes());
    assert_eq!(
        ConversationEvent::decode_canonical(&inverted),
        Err(ConversationEventDecodeError::NonCanonicalBody { tag: 5 })
    );

    // An ended epoch whose generation differs from the retired generation.
    let mut foreign_epoch = encoded_operation(left_operation(Some(epoch(4, 9)), None));
    let epoch_generation_at = 63 + 16;
    foreign_epoch[epoch_generation_at..epoch_generation_at + 8]
        .copy_from_slice(&5_u64.to_be_bytes());
    assert_eq!(
        ConversationEvent::decode_canonical(&foreign_epoch),
        Err(ConversationEventDecodeError::NonCanonicalBody { tag: 5 })
    );
}

#[test]
fn operation_events_replay_in_committed_order_after_genesis() {
    let mut live = validated_shell();
    let mut log = Vec::new();
    for operation in six_operations() {
        let ConversationDecision::Commit(commit) = live.decide_operation(operation) else {
            unreachable!("a validated shell accepts every same-conversation operation");
        };
        log.push(commit.event().encode_canonical());
        live = commit.commit();
    }
    assert_eq!(live.next_event_ordinal(), 7);

    let mut replayed = validated_shell();
    for encoded in &log {
        replayed = replayed
            .replay(ConversationEvent::decode_canonical(encoded).expect("logged event decodes"))
            .expect("the durable log replays in committed order");
    }
    assert_eq!(replayed, live);
}

fn build_operation(kind: u8, values: [u64; 5]) -> ConversationOperation {
    let generation =
        Generation::new((values[0] % 997) + 1).expect("modular generations are nonzero");
    let arbitrary_epoch =
        BindingEpoch::new(ConnectionIncarnation::new(values[1], values[2]), generation);
    match kind % 6 {
        0 => ConversationOperation::Enrolled(
            EnrolledOperation::from_decoded(
                CONVERSATION_ID,
                values[1],
                BindingEpoch::new(
                    ConnectionIncarnation::new(values[2], values[3]),
                    Generation::ONE,
                ),
                values[4],
                values[0],
            )
            .expect("generation-one enrollment bodies are canonical"),
        ),
        1 => ConversationOperation::Attached(AttachedOperation::from_decoded(
            CONVERSATION_ID,
            values[3],
            arbitrary_epoch,
            values[4],
            values[0],
        )),
        2 => ConversationOperation::Detached(DetachedOperation::from_decoded(
            DetachAttemptToken::new(
                values[3]
                    .to_be_bytes()
                    .repeat(2)
                    .try_into()
                    .expect("sixteen token bytes are produced from two eight-byte copies"),
            ),
            CONVERSATION_ID,
            values[1],
            arbitrary_epoch,
            values[4],
            values[0],
        )),
        3 => {
            let left_delivery_seq = (values[4] / 2) + 1;
            let ended_binding_epoch = (values[0] % 2 == 0).then_some(arbitrary_epoch);
            let prior_terminal_delivery_seq =
                (values[1] % 2 == 0).then_some(values[3] % left_delivery_seq);
            let committed = LeaveCommitted::new(
                CONVERSATION_ID,
                LeaveAttemptToken::new(
                    values[2]
                        .to_be_bytes()
                        .repeat(2)
                        .try_into()
                        .expect("sixteen token bytes are produced from two eight-byte copies"),
                ),
                values[1],
                generation,
                ended_binding_epoch,
                prior_terminal_delivery_seq,
                left_delivery_seq,
            )
            .expect("constructed leave results satisfy the canonical invariants");
            ConversationOperation::Left(LeftOperation::new(committed, values[3]))
        }
        4 => ConversationOperation::BindingFate(BindingFateOperation::from_decoded(
            values[3],
            arbitrary_epoch,
            values[4],
        )),
        _ => ConversationOperation::NonzeroDebtAck(NonzeroDebtAckOperation::from_decoded(
            CONVERSATION_ID,
            values[1],
            generation,
            values[4],
        )),
    }
}

proptest! {
    #[test]
    fn any_prefix_replay_produces_a_valid_shell_state(
        raw_operations in proptest::collection::vec(
            (0_u8..6, [any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()]),
            0..12,
        )
    ) {
        let genesis = ConversationGenesis::new(CONVERSATION_ID);
        let mut live = ParticipantConversation::from_genesis(genesis);
        let mut log = Vec::new();

        let ConversationDecision::Commit(commit) = live.decide_genesis_validation() else {
            unreachable!("fresh genesis must select its one-shot event");
        };
        log.push(commit.event().encode_canonical());
        live = commit.commit();

        for (kind, values) in raw_operations {
            let ConversationDecision::Commit(commit) =
                live.decide_operation(build_operation(kind, values))
            else {
                unreachable!("a validated shell accepts every same-conversation operation");
            };
            log.push(commit.event().encode_canonical());
            live = commit.commit();
        }

        for prefix_len in 0..=log.len() {
            let mut replayed = ParticipantConversation::from_genesis(genesis);
            for encoded in &log[..prefix_len] {
                let event = ConversationEvent::decode_canonical(encoded)
                    .expect("every logged event decodes canonically");
                replayed = replayed
                    .replay(event)
                    .expect("every log prefix replays in committed order");
            }
            prop_assert_eq!(replayed.conversation_id(), CONVERSATION_ID);
            prop_assert_eq!(replayed.next_event_ordinal(), prefix_len as u64);
            prop_assert_eq!(replayed.genesis_validated(), prefix_len > 0);
            if prefix_len == log.len() {
                prop_assert_eq!(&replayed, &live);
            }
        }
    }
}
