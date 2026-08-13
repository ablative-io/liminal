//! F8 red-first units, protocol side (`docs/design/F8-MARKER-POISON.md` §3.1,
//! §3.3; the leg's shape is `docs/design/F8-BUILD-LEG-SHAPE.md`).
//!
//! Two defects live at this boundary. §3.1: `validate_binding_fate_floor`
//! computes the post-release floor with **no marker input at all**
//! (`binding_fate.rs:428-442` — its five `floor_transition` arguments are
//! `retained_floor`, `minimum_remaining_cursor`, `candidate_high_watermark`,
//! `hard_observer_progress`, `retained_floor`), while the transition it then
//! calls refuses any floor that crosses a retained marker
//! (`claim_frontier/binding_fate_transition.rs:38-44`). Two halves of one
//! invariant, neither clamping. §3.3: whichever of the five
//! `LiveFrontierError` causes fired, `binding_fate.rs:373` collapses it to the
//! bare name `OwnerTransition` with `.map_err(|_| ..)`.
//!
//! Both units are written to compile BEFORE and AFTER the fix. That is
//! deliberate for §3.3, whose fix changes `OwnerTransition` from a unit
//! variant to a tuple variant: the poles below match it as
//! `OwnerTransition { .. }`, which is a legal pattern for a unit variant AND
//! for a tuple variant, so the red-state tree builds and the fix commit does
//! not get to rewrite the test that judges it.
//!
//! ⚠ THAT LAST GUARD WAS DEPARTED FROM, 2026-08-03, AND THE DEPARTURE IS
//! RECORDED HERE RATHER THAN QUIETLY TAKEN. The §3.3 commit DID change two
//! poles: `a_precedence_refusal_is_told_apart_from_its_four_siblings` was
//! RETIRED (its premise died when §3.1's clamp removed the only route to a
//! `Precedence` refusal on this fixture) and
//! `a_non_precedence_owner_transition_refusal_keeps_its_own_reason` was REMOVED
//! as subsumed. The guard above exists to stop a fix from rewriting its own
//! judge for convenience; what protects that intent here is that BOTH changes
//! were pre-ruled by an independent design gate (5f389528) BEFORE authorship,
//! naming the retirement, its supersession, and the self-naming backstop that
//! replaces it. The author did not choose which tests to change. The
//! `OwnerTransition { .. }` pattern claim above still holds and negative pole
//! ONE, which uses it, is untouched.

use alloc::{
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use crate::{
    algebra::{ResourceVector, WideResourceVector},
    outcome::CandidatePhase,
    wire::{BindingEpoch, ConnectionIncarnation, DeliverySeq, Generation},
};

use super::binding_fate_fixture::{
    TwoExitClaims, two_identity_ledgers, two_identity_order_restore, two_identity_sequence_restore,
};
use super::binding_fate_tests::{frontier_owner_with_limit, ordinary_token};
use super::{
    BindingFateMeasurementError, BindingFateTerminal, BindingTerminalAdmission,
    BindingTerminalCauseClass, LiveFrontierError, LiveFrontierOwner,
};
use crate::lifecycle::{
    ActiveBinding, AdmissionOrder, BindingTerminalDisposition, ClaimFrontiers,
    ClaimFrontiersRestore, ClosureAccounting, ClosureState, CommittedDiedTerminal,
    DiedBindingTransition, FrontierBinding, FrontierParticipant, MarkerProvenance,
    RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge, SealedBindingFateToken,
};

/// The departed peer's permanent index. Distinct from the ordinary token's
/// participant (3) so the two identities never collide in the frontier.
pub(super) const DEPARTED_PEER: u64 = 5;

fn generation(value: u64) -> Result<Generation, String> {
    Generation::new(value).ok_or_else(|| "F8 fixture generation must be nonzero".to_string())
}

fn peer_epoch() -> Result<BindingEpoch, String> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(9, 1),
        generation(1)?,
    ))
}

fn clear_accounting() -> Result<ClosureAccounting, String> {
    ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::default(),
        ResourceVector::new(16, 1024),
        0,
        2,
    )
    .map_err(|error| format!("F8 fixture accounting refused: {error:?}"))
}

/// Mints the exact committed Died terminal the Ordinary token requires, on a
/// throwaway owner. Only the terminal travels; its owner is dropped, so the
/// terminal's provenance never constrains the marker-pinned frontier below.
/// Same construction as `binding_fate_tests::committed_died_owner`.
pub(super) fn committed_died_terminal(
    active: ActiveBinding,
    cursor: DeliverySeq,
) -> Result<CommittedDiedTerminal, String> {
    let high_watermark = cursor
        .checked_add(1)
        .ok_or_else(|| "F8 terminal high watermark overflow".to_string())?;
    let candidate_sequence = high_watermark
        .checked_add(1)
        .ok_or_else(|| "F8 committed terminal sequence overflow".to_string())?;
    let owner = frontier_owner_with_limit(
        active.conversation_id,
        active.participant_id,
        active.binding_epoch,
        cursor,
        high_watermark,
        1,
    )?;
    let prepared = owner
        .prepare_binding_terminal(
            active,
            BindingTerminalCauseClass::Died,
            0,
            candidate_sequence,
            high_watermark,
        )
        .map_err(|refused| {
            format!(
                "F8 committed terminal prepare refused: {:?}",
                refused.error()
            )
        })?;
    let key = prepared.candidate_key();
    let BindingTerminalAdmission::Commit(committed) =
        prepared.admit(key.bind_v3_charge(ResourceVector::new(1, 73)))
    else {
        return Err("F8 Died selector did not commit the ordinary terminal".to_string());
    };
    let (owner, position) = committed.into_parts();
    drop(owner);
    let DiedBindingTransition::Committed(terminal) =
        active.connection_lost(BindingTerminalDisposition::Committed(position))
    else {
        return Err("F8 committed selector position did not produce committed Died".to_string());
    };
    Ok(terminal)
}

/// The incident's frontier, §1, built at the moment P1's fate is measured.
///
/// A compaction marker for P1 is minted and drained, so its retained record
/// sits at `marker_seq` and stays replayable until P1 `MarkerAck`s. P1 never
/// acks (killed), so P1's cursor stays BELOW `marker_seq`. The peer has
/// already departed, and a departing participant's cursor is set to the high
/// watermark — which is now at or past the marker. The floor computation sees
/// only that peer cursor.
///
/// `retained_floor == marker_seq == high_watermark`, so the retained suffix is
/// exactly the one marker row, which is what `validated_retained_records`
/// (`claim_frontier.rs:4958-4968`) demands: the record count must equal
/// `high_watermark + 1 - retained_floor`.
///
/// `charged` selects the refusal class for §3.3's poles. With charges the
/// retained-charge preflight (`live_frontier/binding_fate_transition.rs:23-35`)
/// passes and the frontier transition refuses `Precedence`; without them the
/// preflight itself refuses `RetainedCharge` — a different one of the five
/// causes, on a byte-identical frontier.
fn marker_pinned_owner(
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
    cursor: DeliverySeq,
    marker_seq: DeliverySeq,
    charged: bool,
) -> Result<LiveFrontierOwner, String> {
    if cursor >= marker_seq {
        return Err(
            "F8 fixture needs an UNACKED marker: P1's cursor must sit below it".to_string(),
        );
    }
    let high_watermark = marker_seq;
    let identity_slot_limit = participant_id
        .max(DEPARTED_PEER)
        .checked_add(1)
        .ok_or_else(|| "F8 fixture identity slot limit overflow".to_string())?;

    // Reserve, in the frozen canonical term order (E, T, M, RS, RT, L*T,
    // L*RT, L_other*E) for L=2, T=0, M=0: 2 + 0 + 0 + 0 + 0 + 0 + 0 + 2 = 4.
    // Both identities are Detached, so no binding-terminal claim is owed. That
    // geometry is shared with the PRECEDENCE-CLAMP finalizer units and lives in
    // `binding_fate_fixture`, byte-identical to what stood inline here.
    let claims = TwoExitClaims::above(high_watermark)?;
    let (sequence, order) = two_identity_ledgers(high_watermark)?;

    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, participant_id);
    let marker_record = RetainedCausalRecord {
        delivery_seq: marker_seq,
        admission_order: marker_order,
        kind: RetainedCausalRecordKind::CompactionMarker {
            participant_index: participant_id,
            provenance: MarkerProvenance::NonProductM,
        },
    };

    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id,
            active_identities: vec![
                FrontierParticipant::new(
                    participant_id,
                    cursor,
                    FrontierBinding::Detached(binding_epoch),
                ),
                FrontierParticipant::new(
                    DEPARTED_PEER,
                    high_watermark,
                    FrontierBinding::Detached(peer_epoch()?),
                ),
            ],
            identity_slot_limit,
            retained_floor: u128::from(marker_seq),
            retained_record_limit: 2,
            retained_records: vec![marker_record],
            active_marker_anchors: vec![marker_seq],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: two_identity_sequence_restore(claims, participant_id, DEPARTED_PEER),
            order: two_identity_order_restore(participant_id, DEPARTED_PEER),
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .map_err(|error| format!("F8 fixture frontier refused: {error:?}"))?;

    let retained_charges: Vec<RetainedRecordCharge> = if charged {
        vec![RetainedRecordCharge::new(
            marker_seq,
            marker_order,
            ResourceVector::new(1, 1),
        )]
    } else {
        vec![]
    };

    Ok(LiveFrontierOwner::from_test_parts(
        frontiers,
        clear_accounting()?,
        retained_charges,
        2,
    ))
}

struct IncidentFixture {
    owner: LiveFrontierOwner,
    token: SealedBindingFateToken,
    terminal: CommittedDiedTerminal,
    marker_seq: DeliverySeq,
    hard_observer_progress: DeliverySeq,
}

/// Assembles §1's incident exactly: the ordinary token whose fate is about to
/// be measured, the committed Died terminal it consumes, and the frontier in
/// which the departed peer's cursor has already moved past P1's unacked
/// marker.
fn incident_fixture(charged: bool) -> Result<IncidentFixture, String> {
    let (token, binding, cursor) = ordinary_token()?;
    // M = C + 1: the marker was drained after P1's last ack, so P1's cursor
    // sits strictly below it and the marker is unacked by construction.
    let marker_seq = cursor
        .checked_add(1)
        .ok_or_else(|| "F8 fixture marker sequence overflow".to_string())?;
    let terminal = committed_died_terminal(binding, cursor)?;
    let owner = marker_pinned_owner(
        binding.conversation_id,
        binding.participant_id,
        binding.binding_epoch,
        cursor,
        marker_seq,
        charged,
    )?;
    // The departing peer left its cursor at the high watermark, which is where
    // hard observer progress also stands. This is the input that drives the
    // computed floor past the marker.
    let hard_observer_progress = owner.frontiers().sequence().ledger().high_watermark();
    Ok(IncidentFixture {
        owner,
        token,
        terminal,
        marker_seq,
        hard_observer_progress,
    })
}

/// Runs the measurement and returns only its typed refusal, for the §3.3 poles.
fn refusal_of(charged: bool) -> Result<BindingFateMeasurementError, String> {
    let fixture = incident_fixture(charged)?;
    match fixture.owner.prepare_binding_fate(
        fixture.token,
        BindingFateTerminal::Ordinary(fixture.terminal),
        fixture.hard_observer_progress,
    ) {
        Ok(_) => Err(format!(
            "F8 §3.3 pole expected a refused measurement (charged={charged}) and got a \
             successful one — the fixture no longer mints the refusal it exists to \
             discriminate"
        )),
        Err(refused) => Ok(refused.error()),
    }
}

/// §3.1 RED UNIT. A retained unacked marker PINS the floor.
///
/// Today this fails at the `?` below: the floor is computed from the departed
/// peer's cursor alone, lands one past the marker, and the frontier transition
/// refuses `Precedence` — which `binding_fate.rs:373` then reports as the bare
/// `OwnerTransition`. Pinning is the marker's meaning; unsatisfiability was
/// the bug.
#[test]
fn a_retained_unacked_marker_pins_the_measured_floor() -> Result<(), String> {
    let fixture = incident_fixture(true)?;
    let marker_seq = fixture.marker_seq;
    let prepared = fixture
        .owner
        .prepare_binding_fate(
            fixture.token,
            BindingFateTerminal::Ordinary(fixture.terminal),
            fixture.hard_observer_progress,
        )
        .map_err(|refused| {
            format!(
                "F8 §3.1: a departing peer measured a floor across P1's retained unacked \
                 marker at {marker_seq} and the measurement refused: {:?}",
                refused.error()
            )
        })?;
    let measured_floor = prepared.fate().resulting_floor();
    if measured_floor > marker_seq {
        return Err(format!(
            "F8 §3.1: the measured floor {measured_floor} crossed the retained marker at \
             {marker_seq}"
        ));
    }
    let (owner, _, _) = prepared.into_parts();
    // §5.4 item 2: the marker is still retained, because it is still unacked.
    if !owner
        .frontiers()
        .retained_marker_records()
        .iter()
        .any(|record| record.delivery_seq == marker_seq)
    {
        return Err(format!(
            "F8 §3.1: the pinned marker at {marker_seq} was released by a measurement that \
             was supposed to stop short of it"
        ));
    }
    Ok(())
}

/// §3.3 POSITIVE POLE. A `RetainedCharge` refusal must arrive CARRYING
/// `RetainedCharge`.
///
/// Driven END-TO-END through a real producer, not by poking the constructor:
/// `refusal_of(false)` runs the whole measurement, and the refusal is raised by
/// the retained-charge preflight at
/// `live_frontier/binding_fate_transition.rs:34` — the first guard inside
/// `prepare_binding_fate_transition`, whose `LiveFrontierError` travels out to
/// `binding_fate.rs:373` and is now carried instead of discarded.
///
/// This replaces the Precedence positive pole, whose premise §3.1 retired; see
/// the backstop sentinel below. What that pole tested — that two causes on a
/// byte-identical frontier are TOLD APART at this boundary — is now tested more
/// strongly, by naming the exact cause rather than by comparing two refusals
/// for inequality.
#[test]
fn a_retained_charge_refusal_carries_its_own_cause() -> Result<(), String> {
    let refused = refusal_of(false)?;
    let BindingFateMeasurementError::OwnerTransition(cause) = refused else {
        return Err(format!(
            "F8 §3.3: a retained-charge mismatch did not refuse through the owner transition \
             at all: {refused:?}"
        ));
    };
    if cause != LiveFrontierError::RetainedCharge {
        return Err(format!(
            "F8 §3.3: the owner-transition carrier arrived holding {cause:?}, but the refusal \
             was raised by the retained-charge preflight at \
             binding_fate_transition.rs:34 — the carrier is transporting the wrong cause"
        ));
    }
    Ok(())
}

/// §3.3 BACKSTOP SENTINEL — successor to the RETIRED Precedence positive pole.
///
/// THE RETIREMENT, AND WHAT SUPERSEDED IT. The pole that stood here,
/// `a_precedence_refusal_is_told_apart_from_its_four_siblings`, discriminated a
/// `Precedence` refusal from its four siblings on the §1 incident fixture. Its
/// premise no longer exists. The SOLE producer of
/// `LiveFrontierError::Precedence` is `map_frontier_error` at
/// `live_frontier.rs:1541`, reached only through the floor path that §3.1 now
/// clamps, so the incident fixture no longer mints a Precedence refusal at all
/// — it measures successfully. SUPERSEDED BY §3.1's clamp.
///
/// BOUNDARY, deliberate: §3.1's own unit
/// (`a_retained_unacked_marker_pins_the_measured_floor`) owns the clamp
/// DIRECTION, and this sentinel does not restate it. It asserts nothing about
/// the measured floor's value or about marker retention. Its whole subject is
/// the retired CAUSE.
///
/// WHY A SENTINEL AND NOT A DELETION: a retired pole that simply vanishes is
/// indistinguishable from one quietly deleted, and a silent green here would
/// hide the premise returning. So this stands where the pole stood and, if the
/// premise ever comes back, ANNOUNCES ITSELF BY NAME as the backstop firing.
#[test]
fn the_retired_precedence_pole_backstop_names_itself_if_it_ever_fires() -> Result<(), String> {
    let fixture = incident_fixture(true)?;
    match fixture.owner.prepare_binding_fate(
        fixture.token,
        BindingFateTerminal::Ordinary(fixture.terminal),
        fixture.hard_observer_progress,
    ) {
        Ok(_) => Ok(()),
        Err(refused) => {
            let error = refused.error();
            if matches!(
                error,
                BindingFateMeasurementError::OwnerTransition(LiveFrontierError::Precedence(_))
            ) {
                return Err(format!(
                    "F8 §3.3 BACKSTOP FIRING, BY NAME: the RETIRED Precedence premise has \
                     RETURNED. The §1 incident fixture refused with {error:?}, where §3.1's \
                     clamp should have carried the measurement through. This sentinel \
                     supersedes a_precedence_refusal_is_told_apart_from_its_four_siblings; if \
                     you are reading this, that retirement must be reconsidered — do not \
                     silence this test"
                ));
            }
            Err(format!(
                "F8 §3.3 BACKSTOP, BY NAME: the §1 incident fixture stopped measuring, but NOT \
                 through the retired Precedence premise — it refused with {error:?}. The \
                 retirement still stands; the fixture does not. Report this, do not silence it"
            ))
        }
    }
}

/// §3.3 RED UNIT, negative pole ONE. A failure that is not an owner-transition
/// refusal at all must not wear the carrier. A carrier that answers "yes" to
/// everything is not a carrier, and the positive pole alone cannot detect
/// that.
#[test]
fn a_non_owner_transition_failure_stays_outside_the_owner_transition_carrier() -> Result<(), String>
{
    let fixture = incident_fixture(true)?;
    // A Recovered terminal against an Ordinary token fails the terminal-class
    // check at `binding_fate.rs:409-422`, well before any owner transition.
    let refused = match fixture.owner.prepare_binding_fate(
        fixture.token,
        BindingFateTerminal::Recovered,
        fixture.hard_observer_progress,
    ) {
        Ok(_) => {
            return Err(
                "F8 §3.3 negative pole ONE: a Recovered terminal was accepted for an Ordinary \
                 token"
                    .to_string(),
            );
        }
        Err(refused) => refused.error(),
    };
    if refused != BindingFateMeasurementError::Terminal {
        return Err(format!(
            "F8 §3.3 negative pole ONE: the terminal-class failure changed class: {refused:?}"
        ));
    }
    if matches!(refused, BindingFateMeasurementError::OwnerTransition { .. }) {
        return Err(format!(
            "F8 §3.3 negative pole ONE: a non-owner-transition failure wore the owner-transition \
             carrier: {refused:?}"
        ));
    }
    Ok(())
}

// §3.3 NEGATIVE POLE TWO WAS REMOVED HERE, and the removal is recorded rather
// than performed silently.
//
// It was `a_non_precedence_owner_transition_refusal_keeps_its_own_reason`, and
// it asserted that a RetainedCharge refusal and a Precedence refusal do not
// compare equal at this boundary. BOTH of its premises are gone:
//
//   1. It required a live Precedence refusal from `refusal_of(true)`, which
//      §3.1's clamp retired — the same retirement the backstop sentinel above
//      records.
//   2. Its actual subject, "causes inside the carrier do not collapse to one",
//      is now asserted DIRECTLY and more strongly by
//      `a_retained_charge_refusal_carries_its_own_cause`, which names the exact
//      cause by value instead of comparing two refusals for inequality.
//
// Rewriting it against the surviving cause would have produced a tautology of
// the positive pole (cause == RetainedCharge already entails cause !=
// Precedence), and a tautological test is a green that cannot fail. Negative
// pole ONE below is untouched and still carries the "a carrier that answers yes
// to everything is not a carrier" property.
