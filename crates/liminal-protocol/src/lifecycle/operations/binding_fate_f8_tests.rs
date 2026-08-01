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

use super::binding_fate_tests::{frontier_owner_with_limit, ordinary_token};
use super::{
    BindingFateMeasurementError, BindingFateTerminal, BindingTerminalAdmission,
    BindingTerminalCauseClass, LiveFrontierOwner,
};
use crate::lifecycle::{
    ActiveBinding, AdmissionOrder, BindingTerminalDisposition, ClaimFrontiers,
    ClaimFrontiersRestore, ClosureAccounting, ClosureState, CommittedDiedTerminal,
    DiedBindingTransition, ExitProductRangeRestore, FrontierBinding, FrontierParticipant,
    MarkerProvenance, MovableOrderClaim, MovableSequenceClaim, OrderClaimFrontierRestore,
    OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger, RecoverySequenceReserve,
    RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge,
    SealedBindingFateToken, SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner,
    SequenceLedger, SequenceProductRangesRestore,
};

/// The departed peer's permanent index. Distinct from the ordinary token's
/// participant (3) so the two identities never collide in the frontier.
const DEPARTED_PEER: u64 = 5;

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
fn committed_died_terminal(
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
        .map_err(|refused| format!("F8 committed terminal prepare refused: {:?}", refused.error()))?;
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
/// sits at `marker_seq` and stays replayable until P1 MarkerAcks. P1 never
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
        return Err("F8 fixture needs an UNACKED marker: P1's cursor must sit below it".to_string());
    }
    let high_watermark = marker_seq;
    let identity_slot_limit = participant_id
        .max(DEPARTED_PEER)
        .checked_add(1)
        .ok_or_else(|| "F8 fixture identity slot limit overflow".to_string())?;

    // Reserve, in the frozen canonical term order (E, T, M, RS, RT, L*T,
    // L*RT, L_other*E) for L=2, T=0, M=0: 2 + 0 + 0 + 0 + 0 + 0 + 0 + 2 = 4.
    // Both identities are Detached, so no binding-terminal claim is owed.
    let own_exit = high_watermark
        .checked_add(1)
        .ok_or_else(|| "F8 fixture own exit claim overflow".to_string())?;
    let peer_exit = own_exit
        .checked_add(1)
        .ok_or_else(|| "F8 fixture peer exit claim overflow".to_string())?;
    let own_exit_product = peer_exit
        .checked_add(1)
        .ok_or_else(|| "F8 fixture own exit product overflow".to_string())?;
    let peer_exit_product = own_exit_product
        .checked_add(1)
        .ok_or_else(|| "F8 fixture peer exit product overflow".to_string())?;

    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(2, 0, 0, RecoverySequenceReserve::None),
    )
    .map_err(|error| format!("F8 fixture sequence ledger refused: {error:?}"))?;
    let order = OrderLedger::try_new(
        OrderHigh::Empty,
        OrderClaims::new(0, 2, false, false)
            .map_err(|error| format!("F8 fixture order claims refused: {error:?}"))?,
    )
    .map_err(|error| format!("F8 fixture order ledger refused: {error:?}"))?;

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
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: own_exit,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: participant_id,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: peer_exit,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: DEPARTED_PEER,
                        },
                    },
                ],
                immutable_candidates: vec![],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![
                        ExitProductRangeRestore {
                            start: own_exit_product,
                            length: 1,
                            exit_participant: participant_id,
                        },
                        ExitProductRangeRestore {
                            start: peer_exit_product,
                            length: 1,
                            exit_participant: DEPARTED_PEER,
                        },
                    ],
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![
                    MovableOrderClaim {
                        transaction_order: 0,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: participant_id,
                        },
                    },
                    MovableOrderClaim {
                        transaction_order: 1,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: DEPARTED_PEER,
                        },
                    },
                ],
                immutable_candidates: vec![],
                recovery: None,
            },
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
#[ignore = "TEMPORARY observation scaffolding with a NAMED EXPIRY (design-gate rider \
            130d02fb): reverted in the same landing sequence that carries the F8 fix \
            pieces, and this un-ignored red must be re-observed RED under the priced \
            tier-1 string before any fix commit claims its green"]
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

/// §3.3 RED UNIT, positive pole. A `Precedence` refusal must arrive carrying
/// `Precedence`.
///
/// The observable that survives the variant's shape change is
/// DISCRIMINATION: two different `LiveFrontierError` causes, raised on a
/// byte-identical frontier, must not compare equal at this boundary. Today
/// both are the bare `OwnerTransition`, so they DO compare equal and this
/// fails — which is precisely the tax §2 names, the one that turned a
/// one-minute diagnosis into a store excavation.
#[test]
#[ignore = "TEMPORARY observation scaffolding with a NAMED EXPIRY (design-gate rider \
            130d02fb): reverted in the same landing sequence that carries the F8 fix \
            pieces, and this un-ignored red must be re-observed RED under the priced \
            tier-1 string before any fix commit claims its green"]
fn a_precedence_refusal_is_told_apart_from_its_four_siblings() -> Result<(), String> {
    let precedence = refusal_of(true)?;
    let retained_charge = refusal_of(false)?;
    if !matches!(
        precedence,
        BindingFateMeasurementError::OwnerTransition { .. }
    ) {
        return Err(format!(
            "F8 §3.3: a marker-crossing floor did not refuse through the owner transition at \
             all: {precedence:?}"
        ));
    }
    if precedence == retained_charge {
        return Err(format!(
            "F8 §3.3: a Precedence refusal and a RetainedCharge refusal are indistinguishable \
             at the protocol boundary — both arrive as {precedence:?}, so no operator and no \
             consumer can tell which of the five causes fired"
        ));
    }
    Ok(())
}

/// §3.3 RED UNIT, negative pole ONE. A failure that is not an owner-transition
/// refusal at all must not wear the carrier. A carrier that answers "yes" to
/// everything is not a carrier, and the positive pole alone cannot detect
/// that.
#[test]
fn a_non_owner_transition_failure_stays_outside_the_owner_transition_carrier()
-> Result<(), String> {
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
    if matches!(
        refused,
        BindingFateMeasurementError::OwnerTransition { .. }
    ) {
        return Err(format!(
            "F8 §3.3 negative pole ONE: a non-owner-transition failure wore the owner-transition \
             carrier: {refused:?}"
        ));
    }
    Ok(())
}

/// §3.3 RED UNIT, negative pole TWO. A non-`Precedence` owner-transition
/// refusal keeps its OWN reason. The carrier discriminates by the protocol's
/// own cause, not by the seat that raised it, so a `Precedence` test at any
/// consumer must answer NO for the other four.
#[test]
#[ignore = "TEMPORARY observation scaffolding with a NAMED EXPIRY (design-gate rider \
            130d02fb): reverted in the same landing sequence that carries the F8 fix \
            pieces, and this un-ignored red must be re-observed RED under the priced \
            tier-1 string before any fix commit claims its green"]
fn a_non_precedence_owner_transition_refusal_keeps_its_own_reason() -> Result<(), String> {
    let retained_charge = refusal_of(false)?;
    let precedence = refusal_of(true)?;
    if !matches!(
        retained_charge,
        BindingFateMeasurementError::OwnerTransition { .. }
    ) {
        return Err(format!(
            "F8 §3.3 negative pole TWO: a retained-charge mismatch did not refuse through the \
             owner transition: {retained_charge:?}"
        ));
    }
    if retained_charge == precedence {
        return Err(format!(
            "F8 §3.3 negative pole TWO: a RetainedCharge refusal is readable as lane \
             precedence — both arrive as {retained_charge:?}"
        ));
    }
    Ok(())
}
