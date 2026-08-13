//! A4 (§0.15) body-conflict pins for ordinary record admission.
//!
//! A4 converts exactly ONE of A2's arms: same verified participant, same
//! attempt token, DIFFERENT canonical payload bytes, committed match inside
//! the retained op-log window. That arm warned and committed a second record
//! under A2; under A4 it is a typed `AttemptTokenBodyConflict::RecordAdmission`
//! refusal that commits nothing.
//!
//! # The two ranges, and why the second pin here is the amendment's teeth
//!
//! The dedup branch A4 resolves is entered on a range spanning EVERY
//! participant. Hanging the refusal on that branch is the natural
//! implementation and it is the one A4 permanently outlaws: it would answer a
//! CROSS-participant token hit, and a token-correlated answer across
//! participants is a probe channel that lets one participant test whether
//! another has used a token. So the refusal takes its OWN presenter-scoped
//! range and the wide range stays warn-and-fall-through.
//!
//! [`a_cross_participant_token_hit_still_commits_with_no_refusal`] is the pin
//! that fails against the one-widened-arm build. §0.15 obligation 1 says such
//! a build "violates the cross-participant clause of this amendment regardless
//! of its test results" — this pin makes the test results agree with the law
//! instead of leaving the law unmeasured.
//!
//! # Retention honesty
//!
//! The conflict lookup sees only the retained op-log window, exactly as A2's
//! dedup does: a conflicting re-present arriving after its witness row is
//! compacted commits a second record. That boundary is DECLARED, NOT ARMABLE
//! for the same reason A2's is — no server-side op-log compaction exists for
//! this store today, so there is no mechanism to pin it against. The
//! obligation stays dormant beside A2's, on the same map
//! (`state.rs`'s `committed_admissions`) and therefore on the same trigger:
//! the first compaction mechanism to land arms both at once.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    AttachAttemptToken, AttemptTokenBodyConflict, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    RecordAdmission, RecordAdmissionAttemptToken, RecordCommitted, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

fn open_handler(
    store: &Arc<dyn DurableStore>,
) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    Ok(ProductionParticipantHandler::new(
        Arc::clone(store),
        test_participant_config(),
    )?)
}

fn open_store(data_dir: &Path) -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    open_disk_store_for_tests(data_dir)
}

fn require_enrolled(value: ServerValue) -> Result<EnrollBound, Box<dyn Error>> {
    let ServerValue::EnrollBound(receipt) = value else {
        return Err(format!("A4 fixture enrollment did not bind: {value:?}").into());
    };
    Ok(receipt)
}

fn require_committed(value: ServerValue) -> Result<RecordCommitted, Box<dyn Error>> {
    let ServerValue::RecordCommitted(committed) = value else {
        return Err(format!("A4 fixture admission did not commit: {value:?}").into());
    };
    Ok(committed)
}

fn admission(
    conversation_id: u64,
    member: &EnrollBound,
    generation: Generation,
    token: RecordAdmissionAttemptToken,
    payload: Vec<u8>,
) -> ClientRequest {
    ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id,
        participant_id: member.participant_id(),
        capability_generation: generation,
        record_admission_attempt_token: token,
        payload,
    })
}

/// Every durable row of one conversation's stream as `(sequence, payload)`.
///
/// The store-assigned timestamp is deliberately excluded: it is not a fact the
/// server wrote, and including it would make the comparison measure the clock
/// rather than the refusal.
fn durable_rows(
    store: &Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<(u64, Vec<u8>)>, Box<dyn Error>> {
    let stream_key = format!("liminal:participant-production:{conversation_id}");
    let entries = block_on(store.read_from(&stream_key, 0, 4096))??;
    Ok(entries
        .into_iter()
        .map(|entry| (entry.sequence, entry.payload))
        .collect())
}

/// PIN (i) — the conversion itself. Same participant, same token, different
/// canonical bytes: a typed `AttemptTokenBodyConflict::RecordAdmission`
/// carrying the presenter's own request identity, and NOTHING committed.
///
/// "Commits nothing" is proven three independent ways, because a refusal that
/// merely returns the right shape while consuming authority is the failure this
/// pin exists to catch:
///
/// 1. The conversation's durable rows are byte-identical across the refusal.
/// 2. The next honest admission lands at the sequence the refusal would have
///    taken — no `transaction_order` major and no delivery sequence were burnt.
/// 3. The original identity still answers its own commit afterwards, so the
///    refusal did not disturb the dedup map either.
#[test]
fn same_participant_same_token_different_body_refuses_and_commits_nothing()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let store = open_store(&home.path().join("durability"))?;
    let conversation_id = 9401;
    let connection = ConnectionIncarnation::new(94, 11);
    let handler = open_handler(&store)?;
    let member = require_enrolled(dispatch(
        &handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x61; 16]),
        }),
    )?)?;
    let token = RecordAdmissionAttemptToken::new([0x62; 16]);
    let original_body = vec![0x01, 0x02, 0x03];
    let edited_body = vec![0x01, 0x02, 0x04];
    let first = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            original_body.clone(),
        ),
    )?)?;

    let before = durable_rows(&store, conversation_id)?;
    let refused = dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            edited_body,
        ),
    )?;
    let ServerValue::AttemptTokenBodyConflict(conflict) = refused else {
        return Err(format!(
            "A4: a same-participant same-token different-body presentation must be refused, \
             got {refused:?}"
        )
        .into());
    };
    assert_eq!(
        conflict,
        AttemptTokenBodyConflict::RecordAdmission {
            token,
            conversation_id,
            presented_participant_id: member.participant_id(),
            presented_generation: Generation::ONE,
        },
        "the refusal must echo the presenter's own request identity exactly"
    );

    // 1. Durable bytes are untouched.
    let after = durable_rows(&store, conversation_id)?;
    assert_eq!(
        before, after,
        "the A4 refusal appended durable rows -- it must commit nothing"
    );

    // 2. No sequence and no order major was consumed: the next honest
    //    admission takes the sequence immediately after the first commit.
    let next = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            RecordAdmissionAttemptToken::new([0x63; 16]),
            vec![0x77],
        ),
    )?)?;
    assert_eq!(
        next.delivery_seq(),
        first.delivery_seq() + 1,
        "the refusal consumed a delivery sequence -- it must consume none"
    );

    // 3. The committed identity still answers itself.
    let replay = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            original_body,
        ),
    )?)?;
    assert_eq!(replay.delivery_seq(), first.delivery_seq());
    Ok(())
}

/// PIN (ii) — the probe-channel guard, and the amendment's teeth.
///
/// A DIFFERENT verified participant presenting an already-committed token —
/// here with different bytes too, the strongest form, since it hits the wide
/// range on both of its axes — commits its OWN new record and is never
/// refused. The refusal must be invisible across participants: a refusal here
/// would let any participant test whether any other has used a token.
///
/// This is the pin that goes red against the outlawed one-widened-arm build.
#[test]
fn a_cross_participant_token_hit_still_commits_with_no_refusal() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let store = open_store(&home.path().join("durability"))?;
    let conversation_id = 9402;
    let first_connection = ConnectionIncarnation::new(94, 12);
    let second_connection = ConnectionIncarnation::new(94, 13);
    let third_connection = ConnectionIncarnation::new(94, 17);
    let handler = open_handler(&store)?;
    let first_member = require_enrolled(dispatch(
        &handler,
        first_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x64; 16]),
        }),
    )?)?;
    let second_member = require_enrolled(dispatch(
        &handler,
        second_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x65; 16]),
        }),
    )?)?;
    let third_member = require_enrolled(dispatch(
        &handler,
        third_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x6C; 16]),
        }),
    )?)?;
    let token = RecordAdmissionAttemptToken::new([0x66; 16]);
    let first = require_committed(dispatch(
        &handler,
        first_connection,
        admission(
            conversation_id,
            &first_member,
            Generation::ONE,
            token,
            vec![0x11, 0x12],
        ),
    )?)?;

    // Different participant, different bytes: a wide-range hit on both axes.
    let foreign = require_committed(dispatch(
        &handler,
        second_connection,
        admission(
            conversation_id,
            &second_member,
            Generation::ONE,
            token,
            vec![0x11, 0x13],
        ),
    )?)?;
    assert!(foreign.delivery_seq() > first.delivery_seq());
    assert_eq!(
        foreign.sender_participant_id(),
        second_member.participant_id()
    );

    // A THIRD participant, IDENTICAL bytes: still a dedup miss, still a
    // commit. Keyed to the presenter, the entry cannot be hit from outside.
    // The third member exists because the second is no longer a clean
    // cross-participant probe -- it has spent this token itself now, so its
    // next differing body is its OWN conflict, which is the same
    // discrimination read from the other side.
    let twin = require_committed(dispatch(
        &handler,
        third_connection,
        admission(
            conversation_id,
            &third_member,
            Generation::ONE,
            token,
            vec![0x11, 0x12],
        ),
    )?)?;
    assert!(twin.delivery_seq() > foreign.delivery_seq());
    assert_eq!(twin.sender_participant_id(), third_member.participant_id());

    // And the first participant's own conflicting re-present IS refused, in
    // the same conversation, at the same instant: the discrimination is by
    // presenter and by nothing else.
    let refused = dispatch(
        &handler,
        first_connection,
        admission(
            conversation_id,
            &first_member,
            Generation::ONE,
            token,
            vec![0x11, 0x14],
        ),
    )?;
    assert!(
        matches!(refused, ServerValue::AttemptTokenBodyConflict(_)),
        "the presenter's own conflict must still refuse: {refused:?}"
    );
    Ok(())
}

/// PIN (iii) — the normal dedup answer survives the new arm.
///
/// A same-token same-bytes re-present is A2's ordinary answer-lost recovery
/// and must never be converted into a refusal. This pin fixes the ORDER of the
/// two lookups: the exact-identity hit is answered before the presenter-scoped
/// range is probed at all, so an implementation that probes the range first
/// (and refuses on its own committed identity) goes red here.
#[test]
fn identical_bytes_re_present_still_dedups_after_the_conflict_arm() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let store = open_store(&home.path().join("durability"))?;
    let conversation_id = 9403;
    let connection = ConnectionIncarnation::new(94, 14);
    let handler = open_handler(&store)?;
    let member = require_enrolled(dispatch(
        &handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x67; 16]),
        }),
    )?)?;
    let token = RecordAdmissionAttemptToken::new([0x68; 16]);
    let body = vec![0xB7, 0x03, 0x5C];
    let first = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            body.clone(),
        ),
    )?)?;
    let replay = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            body.clone(),
        ),
    )?)?;
    assert_eq!(replay.delivery_seq(), first.delivery_seq());
    assert_eq!(replay.sender_participant_id(), first.sender_participant_id());

    // And it still dedups after the same token has been refused for a
    // different body: the refusal writes nothing to the map.
    let refused = dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            vec![0xB7, 0x03, 0x5D],
        ),
    )?;
    assert!(
        matches!(refused, ServerValue::AttemptTokenBodyConflict(_)),
        "expected the A4 refusal between the two dedup answers: {refused:?}"
    );
    let after_refusal = require_committed(dispatch(
        &handler,
        connection,
        admission(conversation_id, &member, Generation::ONE, token, body),
    )?)?;
    assert_eq!(after_refusal.delivery_seq(), first.delivery_seq());
    Ok(())
}

/// PIN (iv) — the ordering A2 fixed and A4 inherits: the conflict lookup runs
/// AFTER authority verification.
///
/// A presenter whose generation is stale is answered by the authority
/// classification, never by the token arm. Moving the cheap token lookup ahead
/// of authority would hand an unauthorized presenter a token oracle — the
/// exact disclosure A4's refusal is safe from only because the presenter has
/// already been proven to BE the committed identity's own participant.
#[test]
fn a_stale_presenter_is_answered_by_authority_never_by_the_token_arm()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let store = open_store(&home.path().join("durability"))?;
    let conversation_id = 9404;
    let connection = ConnectionIncarnation::new(94, 15);
    let handler = open_handler(&store)?;
    let member = require_enrolled(dispatch(
        &handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x69; 16]),
        }),
    )?)?;
    let token = RecordAdmissionAttemptToken::new([0x6A; 16]);
    require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            vec![0x21, 0x22],
        ),
    )?)?;

    // Rotate the generation, so generation ONE is now stale authority.
    let reattach_connection = ConnectionIncarnation::new(94, 16);
    let attached = dispatch(
        &handler,
        reattach_connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: member.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: member.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x6B; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("A4 ordering fixture did not reattach: {attached:?}").into());
    };
    assert!(
        attached.capability_generation().get() > 1,
        "the pin requires a real rotation to make generation ONE stale"
    );

    let stale = dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            vec![0x21, 0x23],
        ),
    )?;
    assert!(
        !matches!(stale, ServerValue::AttemptTokenBodyConflict(_)),
        "a stale presenter reached the token arm -- the lookup must run after \
         authority verification: {stale:?}"
    );
    assert!(
        matches!(stale, ServerValue::StaleAuthority(_)),
        "expected the authority classification to answer the stale presenter: {stale:?}"
    );
    Ok(())
}

/// §0.15 build obligation 2 — the named-not-measured latency channel, measured.
///
/// # What is being measured, and why this scope is the whole differential
///
/// A4's refusal reads a presenter-scoped range; A2's surviving warn arm reads
/// a wide one. The amendment names the differential as a timing signal in
/// principle: "a presenter-scoped lookup that is faster than the wide one is a
/// timing signal", and requires the build to measure it and bound or unify it.
///
/// The two probes differ in EXACTLY one operation — the `BTreeMap::range(..)`
/// bounds handed to `answer_committed_record_admission`. Everything else on
/// the admission path (decode, authority classification, envelope
/// construction, response encode) is byte-identical between the two arms and
/// runs before the branch, so measuring the map primitive measures the entire
/// differential and nothing else. That is why this measures the map directly
/// rather than dispatching 10k admissions through the handler: the durable
/// append cost would dominate by orders of magnitude and hide the very
/// quantity under measurement.
///
/// # Setup
///
/// 10,000 committed identities in one map — 200 participants x 50 tokens
/// each — plus one HOT token committed by all 200 participants, so the wide
/// range over that token spans 200 entries while the presenter-scoped range
/// over it spans exactly 1. That is the worst case for a differential: if the
/// two are indistinguishable HERE, they are indistinguishable everywhere.
///
/// Run it:
///
/// ```text
/// cargo test -p liminal-server --release --lib -- --ignored --nocapture \
///     a4_presenter_scoped_versus_wide_lookup_latency
/// ```
///
/// Ignored because it is a measurement, not a pin: it asserts only that both
/// probes agree on their ANSWER (which is a real invariant), never a wall-clock
/// bound, because a timing assertion on shared CI hardware is a flake generator
/// and would be a worse instrument than the recorded numbers.
/// Numbers and verdict live in `gate-logs/breaking-window/leg3-a4-latency.md`.
#[test]
#[ignore = "latency measurement for §0.15 obligation 2; run explicitly with --ignored"]
fn a4_presenter_scoped_versus_wide_lookup_latency() {
    use std::collections::BTreeMap;
    use std::hint::black_box;
    use std::time::Instant;

    use liminal_protocol::wire::ParticipantId;

    use super::state::CommittedAdmissionKey;

    const PARTICIPANTS: u64 = 200;
    const TOKENS_EACH: u64 = 50;
    const TRIALS: u32 = 200_000;

    fn token_of(index: u64) -> [u8; 16] {
        let mut token = [0_u8; 16];
        token[..8].copy_from_slice(&index.to_be_bytes());
        token
    }
    fn fingerprint_of(index: u64) -> [u8; 32] {
        let mut fingerprint = [0_u8; 32];
        fingerprint[..8].copy_from_slice(&index.to_be_bytes());
        fingerprint
    }

    let mut map: BTreeMap<CommittedAdmissionKey, u64> = BTreeMap::new();
    let hot_token = token_of(u64::MAX);
    for participant in 0..PARTICIPANTS {
        for token_index in 0..TOKENS_EACH {
            let key = participant * TOKENS_EACH + token_index;
            map.insert(
                (token_of(key), participant, fingerprint_of(key)),
                key,
            );
        }
        // The hot token: every participant has committed under it, so the
        // wide range spans 200 entries and the presenter-scoped range spans 1.
        map.insert((hot_token, participant, fingerprint_of(participant)), participant);
    }
    let population = map.len();

    let presenter_range = |token: [u8; 16], presenter: ParticipantId| {
        map.range((token, presenter, [0_u8; 32])..=(token, presenter, [0xFF_u8; 32]))
            .next()
            .is_some()
    };
    let wide_range = |token: [u8; 16]| {
        map.range(
            (token, ParticipantId::MIN, [0_u8; 32])..=(token, ParticipantId::MAX, [0xFF_u8; 32]),
        )
        .next()
        .is_some()
    };

    // The answers must agree wherever both are defined, which is the only
    // property this measurement is allowed to assert.
    assert!(presenter_range(hot_token, 7) && wide_range(hot_token));
    let cold_token = token_of(u64::MAX - 1);
    assert!(!presenter_range(cold_token, 7) && !wide_range(cold_token));

    let mut timings = Vec::new();
    for (label, probe) in [
        (
            "presenter-scoped HIT  (hot token, 1 of 200 entries)",
            Box::new(|| presenter_range(hot_token, 7)) as Box<dyn Fn() -> bool>,
        ),
        (
            "wide           HIT  (hot token, 200 entries spanned)",
            Box::new(|| wide_range(hot_token)),
        ),
        (
            "presenter-scoped MISS (uncommitted token)",
            Box::new(|| presenter_range(cold_token, 7)),
        ),
        (
            "wide           MISS (uncommitted token)",
            Box::new(|| wide_range(cold_token)),
        ),
    ] {
        // One warm pass before timing, so the first probe measured does not
        // also pay for the map's cache being cold.
        for _ in 0..10_000 {
            black_box(probe());
        }
        let started = Instant::now();
        for _ in 0..TRIALS {
            black_box(probe());
        }
        let elapsed = started.elapsed();
        #[expect(clippy::cast_precision_loss, reason = "nanosecond report only")]
        let per_probe = elapsed.as_nanos() as f64 / f64::from(TRIALS);
        timings.push((label, per_probe));
    }

    println!("A4 latency measurement -- population {population} committed identities");
    println!("  trials per probe class: {TRIALS}");
    for (label, per_probe) in &timings {
        println!("  {label}: {per_probe:.2} ns/probe");
    }
    let hit_delta = timings[0].1 - timings[1].1;
    let miss_delta = timings[2].1 - timings[3].1;
    println!("  HIT  differential (presenter - wide): {hit_delta:+.2} ns");
    println!("  MISS differential (presenter - wide): {miss_delta:+.2} ns");
}

/// §0.15 obligation 2, the sharper half: is the cross-participant collision
/// observable in the PRESENTER'S OWN response time?
///
/// The map-primitive measurement above answers the obligation's literal
/// question (presenter-scoped range vs wide range). This one answers the
/// question the obligation exists for. A4 leaves A2's cross-participant arm
/// warn-and-fall-through, so a presenter whose token happens to be held by
/// SOMEBODY ELSE pays the wide range's hit instead of its miss AND a
/// `tracing::warn!` — and if that were observable at the presenter, the
/// permanently-outlawed probe channel would exist through timing even though
/// no byte of it reaches the wire.
///
/// The measurement is therefore end to end at the dispatch seam: the same
/// participant admitting under tokens NOBODY holds versus tokens ANOTHER
/// participant holds. Both commit; the difference between them is the entire
/// channel.
///
/// Run it:
///
/// ```text
/// cargo test -p liminal-server --release --lib -- --ignored --nocapture \
///     a4_cross_participant_collision_is_not_observable_in_response_time
/// ```
///
/// Ignored and assertion-free about wall clock for the same reason as its
/// sibling. Numbers and verdict in `gate-logs/breaking-window/leg3-a4-latency.md`.
#[test]
#[ignore = "latency measurement for §0.15 obligation 2; run explicitly with --ignored"]
fn a4_cross_participant_collision_is_not_observable_in_response_time()
-> Result<(), Box<dyn Error>> {
    use std::time::Instant;

    const SAMPLES: u64 = 400;

    let home = tempfile::tempdir()?;
    let store = open_store(&home.path().join("durability"))?;
    let conversation_id = 9405;
    let victim_connection = ConnectionIncarnation::new(94, 20);
    let prober_connection = ConnectionIncarnation::new(94, 21);
    let handler = open_handler(&store)?;
    let victim = require_enrolled(dispatch(
        &handler,
        victim_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x71; 16]),
        }),
    )?)?;
    let prober = require_enrolled(dispatch(
        &handler,
        prober_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x72; 16]),
        }),
    )?)?;

    let held = |index: u64| {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&index.to_be_bytes());
        bytes[15] = 0xAA;
        RecordAdmissionAttemptToken::new(bytes)
    };
    let fresh = |index: u64| {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&index.to_be_bytes());
        bytes[15] = 0xBB;
        RecordAdmissionAttemptToken::new(bytes)
    };

    // The victim spends every `held` token first, so the prober's later
    // presentation of the same token is a genuine cross-participant hit.
    for index in 0..SAMPLES {
        require_committed(dispatch(
            &handler,
            victim_connection,
            admission(
                conversation_id,
                &victim,
                Generation::ONE,
                held(index),
                vec![0x01],
            ),
        )?)?;
    }

    let measure = |mint: &dyn Fn(u64) -> RecordAdmissionAttemptToken| -> Result<f64, Box<dyn Error>> {
        let started = Instant::now();
        for index in 0..SAMPLES {
            require_committed(dispatch(
                &handler,
                prober_connection,
                admission(
                    conversation_id,
                    &prober,
                    Generation::ONE,
                    mint(index),
                    vec![0x02],
                ),
            )?)?;
        }
        let elapsed = started.elapsed();
        #[expect(clippy::cast_precision_loss, reason = "nanosecond report only")]
        let per_dispatch = elapsed.as_nanos() as f64 / SAMPLES as f64;
        Ok(per_dispatch)
    };

    // Interleaved order (fresh, colliding, fresh again) so a monotone drift in
    // the store -- which grows with every commit -- cannot masquerade as the
    // differential. The second fresh pass is the control on the first.
    let fresh_first = measure(&|index| fresh(index))?;
    let colliding = measure(&|index| held(index))?;
    let fresh_second = measure(&|index| fresh(index + SAMPLES))?;

    println!("A4 cross-participant observability -- {SAMPLES} dispatches per class");
    println!("  fresh token, pass 1      : {fresh_first:.0} ns/dispatch");
    println!("  COLLIDING token (wide hit + warn): {colliding:.0} ns/dispatch");
    println!("  fresh token, pass 2      : {fresh_second:.0} ns/dispatch");
    let drift = fresh_second - fresh_first;
    let signal = colliding - (fresh_first + fresh_second) / 2.0;
    println!("  store drift across the two fresh passes: {drift:+.0} ns");
    println!("  collision signal vs the fresh mean      : {signal:+.0} ns");
    Ok(())
}
