#![allow(clippy::expect_used, clippy::panic)]

use alloc::vec;

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
    unknown_kind[25] = 2;
    assert_eq!(
        ConversationEvent::decode_canonical(&unknown_kind),
        Err(ConversationEventDecodeError::UnknownEventKind { tag: 2 })
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
