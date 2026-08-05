//! PRECEDENCE-CLAMP red-first units: the SECOND enforcer
//! (`docs/design/PRECEDENCE-CLAMP.md` M1, M1a, M6, gate G2).
//!
//! F8 §3.1 clamped the floor where it is MEASURED
//! (`validate_binding_fate_floor`). It did not touch the place that same floor
//! is enforced a second time. `PendingDiedOrdinaryFinalizer` carries a FROZEN
//! `resulting_floor` fixed at measurement (`binding_fate.rs`, the two
//! `prepare_pending_died_*` producers), and
//! `complete_pending_died_ordinary_finalizer` replays that stored value into
//! `install_finalized_binding_fate_floor`, which re-checks it against the
//! frontier as it stands AT COMPLETION. Between those two moments the server
//! holds the finalizer as a value across an enclosing transition
//! (`liminal-server/.../state.rs`'s `PreparedOrdinaryFinalizer`), so the two
//! frontiers are not the same frontier.
//!
//! `install_finalized_binding_fate_floor` refuses on TWO conditions, and the
//! units below drive one each:
//!
//!   * `ResultingFrontier` — the measured floor now sits BELOW the current
//!     retained floor, which advanced by ordinary means while the finalizer
//!     waited. Needs no marker at all.
//!   * `Precedence` — a retained marker now sits strictly below the measured
//!     floor. `marker_records` gains rows only through marker drain, so this is
//!     the shape a marker candidate produces when it is drained after the
//!     measurement that could not see it.
//!
//! Both refusals are permanent: the enclosing source row is already durable, so
//! the completion is retried on every boot and refuses identically forever.
//! Neither unit routes through `cap_floor` and neither weakens the marker rule;
//! each asserts that a floor arriving at the second enforcer is made LEGAL
//! there rather than refused there.

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

use super::LiveFrontierOwner;
use super::binding_fate_f8_tests::{DEPARTED_PEER, committed_died_terminal};
use super::binding_fate_tests::ordinary_token;
use crate::lifecycle::{
    AdmissionOrder, ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting, ClosureState,
    CommittedDiedTerminal, ExitProductRangeRestore, FrontierBinding, FrontierParticipant,
    MarkerProvenance, MovableOrderClaim, MovableSequenceClaim, OrderClaimFrontierRestore,
    OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger, PendingDiedOrdinaryFinalizer,
    RecoverySequenceReserve, RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge,
    SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner, SequenceLedger,
    SequenceProductRangesRestore,
};

/// Which single row occupies the one-row retained suffix of a fixture frontier.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SuffixRow {
    /// An unacked compaction marker for the fate's own participant. It PINS the
    /// floor: the enforcers refuse any floor strictly above it.
    Marker,
    /// An ordinary record. Carries no precedence meaning at all, so a frontier
    /// holding one leaves the floor free to advance to `high_watermark + 1`.
    Ordinary,
}

fn generation(value: u64) -> Result<Generation, String> {
    Generation::new(value).ok_or_else(|| "finalizer fixture generation must be nonzero".to_string())
}

fn peer_epoch() -> Result<BindingEpoch, String> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(9, 1),
        generation(1)?,
    ))
}

/// The exact charge every fixture frontier's single retained row carries.
const SUFFIX_CHARGE: ResourceVector = ResourceVector::new(1, 1);

/// Accounting whose baseline is exactly the one retained row's charge.
///
/// This has to be the real sum, not zero: releasing a row subtracts its charge
/// from the baseline (`live_frontier/state.rs`'s `accounting_after_floor`), so a
/// zero baseline makes every floor advance that releases anything refuse
/// `ClosureAccounting` — a refusal about bookkeeping, not about precedence,
/// which would mask the defect these units exist to measure.
fn clear_accounting() -> Result<ClosureAccounting, String> {
    ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(
            u128::from(SUFFIX_CHARGE.entries),
            u128::from(SUFFIX_CHARGE.bytes),
        ),
        ResourceVector::new(16, 1024),
        0,
        2,
    )
    .map_err(|error| format!("finalizer fixture accounting refused: {error:?}"))
}

/// One two-identity frontier whose retained suffix is exactly one row at
/// `suffix_seq`, with `retained_floor == high_watermark == suffix_seq`.
///
/// Same geometry as the F8 incident fixture, generalised over the row's kind
/// and its sequence so a BEFORE frontier and an AFTER frontier can be built
/// from one place. The departed peer's cursor sits at the high watermark, which
/// is the input that drives the computed floor to `suffix_seq + 1`; the fate's
/// own participant sits below it at `cursor`, unacked.
fn suffix_frontier(
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
    cursor: DeliverySeq,
    suffix_seq: DeliverySeq,
    row: SuffixRow,
) -> Result<LiveFrontierOwner, String> {
    if cursor >= suffix_seq {
        return Err(
            "finalizer fixture needs an UNACKED suffix: the cursor must sit below it".to_string(),
        );
    }
    let high_watermark = suffix_seq;
    let identity_slot_limit = participant_id
        .max(DEPARTED_PEER)
        .checked_add(1)
        .ok_or_else(|| "finalizer fixture identity slot limit overflow".to_string())?;

    // Frozen canonical claim order (E, T, M, RS, RT, L*T, L*RT, L_other*E) for
    // L=2, T=0, M=0: two exits and two exit products. Both identities are
    // Detached, so no binding-terminal claim is owed.
    let own_exit = high_watermark
        .checked_add(1)
        .ok_or_else(|| "finalizer fixture own exit claim overflow".to_string())?;
    let peer_exit = own_exit
        .checked_add(1)
        .ok_or_else(|| "finalizer fixture peer exit claim overflow".to_string())?;
    let own_exit_product = peer_exit
        .checked_add(1)
        .ok_or_else(|| "finalizer fixture own exit product overflow".to_string())?;
    let peer_exit_product = own_exit_product
        .checked_add(1)
        .ok_or_else(|| "finalizer fixture peer exit product overflow".to_string())?;

    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(2, 0, 0, RecoverySequenceReserve::None),
    )
    .map_err(|error| format!("finalizer fixture sequence ledger refused: {error:?}"))?;
    let order = OrderLedger::try_new(
        OrderHigh::Empty,
        OrderClaims::new(0, 2, false, false)
            .map_err(|error| format!("finalizer fixture order claims refused: {error:?}"))?,
    )
    .map_err(|error| format!("finalizer fixture order ledger refused: {error:?}"))?;

    let (phase, kind) = match row {
        SuffixRow::Marker => (
            CandidatePhase::CompactionMarker,
            RetainedCausalRecordKind::CompactionMarker {
                participant_index: participant_id,
                provenance: MarkerProvenance::NonProductM,
            },
        ),
        SuffixRow::Ordinary => (
            CandidatePhase::OrdinaryRecord,
            RetainedCausalRecordKind::OrdinaryRecord {
                participant_index: participant_id,
            },
        ),
    };
    let suffix_order = AdmissionOrder::new(0, phase, participant_id);
    let suffix_record = RetainedCausalRecord {
        delivery_seq: suffix_seq,
        admission_order: suffix_order,
        kind,
    };
    let active_marker_anchors = match row {
        SuffixRow::Marker => vec![suffix_seq],
        SuffixRow::Ordinary => vec![],
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
            retained_floor: u128::from(suffix_seq),
            retained_record_limit: 2,
            retained_records: vec![suffix_record],
            active_marker_anchors,
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
    .map_err(|error| format!("finalizer fixture frontier refused: {error:?}"))?;

    let retained_charges: Vec<RetainedRecordCharge> = vec![RetainedRecordCharge::new(
        suffix_seq,
        suffix_order,
        SUFFIX_CHARGE,
    )];

    Ok(LiveFrontierOwner::from_test_parts(
        frontiers,
        clear_accounting()?,
        retained_charges,
        2,
    ))
}

/// One measured Ordinary finalizer, plus the floor it froze.
struct MeasuredFinalizer {
    finalizer: PendingDiedOrdinaryFinalizer,
    measured_floor: DeliverySeq,
}

/// Runs the real measurement that mints a `PendingDiedOrdinaryFinalizer`, on a
/// BEFORE frontier whose retained suffix is `row`.
///
/// This is the production producer (`prepare_pending_died_ordinary_finalizer`),
/// not a hand-built value: the frozen floor under test is the one the protocol
/// itself mints, so a unit below cannot pass by inventing a floor the protocol
/// would never produce.
fn measured_finalizer(
    row: SuffixRow,
    suffix_seq_from_cursor: u64,
) -> Result<MeasuredFinalizer, String> {
    let (token, binding, cursor) = ordinary_token()?;
    let suffix_seq = cursor
        .checked_add(suffix_seq_from_cursor)
        .ok_or_else(|| "finalizer fixture suffix sequence overflow".to_string())?;
    let terminal: CommittedDiedTerminal = committed_died_terminal(binding, cursor)?;
    let owner = suffix_frontier(
        binding.conversation_id,
        binding.participant_id,
        binding.binding_epoch,
        cursor,
        suffix_seq,
        row,
    )?;
    let hard_observer_progress = owner.frontiers().sequence().ledger().high_watermark();
    let prepared = owner
        .prepare_pending_died_ordinary_finalizer(token, terminal, hard_observer_progress)
        .map_err(|refused| {
            format!(
                "PRECEDENCE-CLAMP fixture: the finalizer measurement itself refused: {:?}",
                refused.error()
            )
        })?;
    let (_, fate, finalizer) = prepared.into_parts();
    Ok(MeasuredFinalizer {
        measured_floor: fate.resulting_floor(),
        finalizer,
    })
}

/// M1a RED UNIT — the subsumed floor, and it needs NO marker.
///
/// The finalizer is measured against a frontier whose floor is `M`. While it
/// waits, the retained floor advances by ordinary means to `M + 1` — every
/// producer of a floor advance does exactly this, and none of them consults a
/// pending finalizer. The frozen floor is now BELOW the current retained floor,
/// so `install_finalized_binding_fate_floor`'s first guard refuses
/// `ResultingFrontier`.
///
/// The fate's floor is SUBSUMED: the frontier is already past it, so installing
/// the older value is meaningless. That is a no-op, not a refusal, and the
/// refusal is a permanent brick because the enclosing source row is durable.
#[test]
fn a_floor_subsumed_while_the_finalizer_waited_installs_as_a_no_op() -> Result<(), String> {
    let (_, binding, cursor) = ordinary_token()?;
    let measured = measured_finalizer(SuffixRow::Marker, 1)?;
    let marker_seq = cursor
        .checked_add(1)
        .ok_or_else(|| "fixture marker sequence overflow".to_string())?;
    if measured.measured_floor != marker_seq {
        return Err(format!(
            "PRECEDENCE-CLAMP M1a: the fixture's frozen floor is {} and not the pinned marker at \
             {marker_seq} — the premise moved",
            measured.measured_floor
        ));
    }

    // THE INTERVAL. The floor advanced one past the fate's measured floor while
    // the finalizer was held. The marker it was pinned to is gone, pruned by
    // that same advance, so this frontier carries no precedence obligation at
    // all — the only thing that changed is that the floor moved on.
    let advanced_seq = marker_seq
        .checked_add(1)
        .ok_or_else(|| "fixture advanced sequence overflow".to_string())?;
    let advanced = suffix_frontier(
        binding.conversation_id,
        binding.participant_id,
        binding.binding_epoch,
        cursor,
        advanced_seq,
        SuffixRow::Ordinary,
    )?;
    let retained_floor_before = advanced.frontiers().retained_floor();

    let owner = advanced
        .complete_pending_died_ordinary_finalizer(measured.finalizer)
        .map_err(|error| {
            format!(
                "PRECEDENCE-CLAMP M1a: a fate floor of {} was subsumed by a retained floor that \
                 had already advanced to {retained_floor_before}, and the finalizer refused \
                 {error:?} instead of treating it as the no-op it is. The enclosing source row is \
                 durable, so this refusal repeats on every boot, forever",
                measured.measured_floor
            )
        })?;

    let retained_floor_after = owner.frontiers().retained_floor();
    if retained_floor_after != retained_floor_before {
        return Err(format!(
            "PRECEDENCE-CLAMP M1a: a subsumed fate floor moved the retained floor from \
             {retained_floor_before} to {retained_floor_after}. A subsumed floor installs NOTHING; \
             driving it backwards would eat rows the frontier still owes"
        ));
    }
    Ok(())
}

/// M1/M6 RED UNIT — a marker below the frozen floor, at the SECOND enforcer.
///
/// The finalizer is measured against a frontier holding an ordinary row, so
/// nothing pins the floor and the measurement mints `M + 1`. While it waits, a
/// marker becomes retained at `M`: `marker_records` gains rows only through
/// marker drain, so this is the state a marker candidate leaves behind when it
/// is drained after a measurement that could not see it — `retained_marker_records`
/// reports drained markers only.
///
/// The frozen floor now crosses a retained marker and the second enforcer
/// refuses `Precedence` — the same refusal §3.1 removed from the measurement
/// path, arriving through the door §3.1 did not close. M6's shape: construct a
/// retained marker BELOW the naively-computed floor and assert the fate ADMITS.
/// The clamp target is the marker itself, never `marker - 1`: the enforcer
/// refuses on strictly-below, and `install_binding_fate_transition` retains
/// markers `>= resulting_floor`, so a marker sitting exactly at the floor
/// survives its own pin.
#[test]
fn a_marker_retained_while_the_finalizer_waited_pins_the_installed_floor() -> Result<(), String> {
    let (_, binding, cursor) = ordinary_token()?;
    let measured = measured_finalizer(SuffixRow::Ordinary, 1)?;
    let marker_seq = cursor
        .checked_add(1)
        .ok_or_else(|| "fixture marker sequence overflow".to_string())?;
    let expected_floor = marker_seq
        .checked_add(1)
        .ok_or_else(|| "fixture measured floor overflow".to_string())?;
    if measured.measured_floor != expected_floor {
        return Err(format!(
            "PRECEDENCE-CLAMP M1: the unpinned fixture froze a floor of {} rather than \
             {expected_floor} — the premise moved",
            measured.measured_floor
        ));
    }

    // THE INTERVAL. The marker candidate that was invisible to the measurement
    // is now a retained marker record at `marker_seq`, one below the frozen
    // floor.
    let pinned = suffix_frontier(
        binding.conversation_id,
        binding.participant_id,
        binding.binding_epoch,
        cursor,
        marker_seq,
        SuffixRow::Marker,
    )?;

    let owner = pinned
        .complete_pending_died_ordinary_finalizer(measured.finalizer)
        .map_err(|error| {
            format!(
                "PRECEDENCE-CLAMP M1: a frozen floor of {} crossed a marker retained at \
                 {marker_seq} while the finalizer waited, and the second enforcer refused \
                 {error:?}. The enclosing source row is durable, so this refusal repeats on every \
                 boot, forever",
                measured.measured_floor
            )
        })?;

    let installed = owner.frontiers().retained_floor();
    if installed != u128::from(marker_seq) {
        return Err(format!(
            "PRECEDENCE-CLAMP M1/M2: the installed floor is {installed}, not the pinned marker at \
             {marker_seq}. Clamping short of the marker silently destroys a legal floor advance; \
             clamping past it eats the marker"
        ));
    }
    if !owner
        .frontiers()
        .retained_marker_records()
        .iter()
        .any(|record| record.delivery_seq == marker_seq)
    {
        return Err(format!(
            "PRECEDENCE-CLAMP M1: the marker at {marker_seq} was released by a finalizer that was \
             supposed to stop exactly at it"
        ));
    }
    Ok(())
}

/// F5 GUARD — a finalizer whose floor is legal at completion installs exactly
/// the floor it measured, byte for byte.
///
/// The clamp may only change the outcome where a floor would previously have
/// been REFUSED. This is the majority case: nothing pins the floor, nothing
/// advanced past it, and the empty marker set must resolve to "no clamp" rather
/// than to a guess about the minimum of an empty set.
#[test]
fn an_uncontested_finalizer_floor_installs_unchanged() -> Result<(), String> {
    let (_, binding, cursor) = ordinary_token()?;
    let measured = measured_finalizer(SuffixRow::Ordinary, 1)?;
    let suffix_seq = cursor
        .checked_add(1)
        .ok_or_else(|| "fixture suffix sequence overflow".to_string())?;
    let unchanged = suffix_frontier(
        binding.conversation_id,
        binding.participant_id,
        binding.binding_epoch,
        cursor,
        suffix_seq,
        SuffixRow::Ordinary,
    )?;

    let owner = unchanged
        .complete_pending_died_ordinary_finalizer(measured.finalizer)
        .map_err(|error| {
            format!("PRECEDENCE-CLAMP F5: an uncontested finalizer floor refused: {error:?}")
        })?;
    let installed = owner.frontiers().retained_floor();
    if installed != u128::from(measured.measured_floor) {
        return Err(format!(
            "PRECEDENCE-CLAMP F5: an uncontested floor of {} installed as {installed}. Marker-free \
             floors must be byte-identical to the pre-clamp tree",
            measured.measured_floor
        ));
    }
    Ok(())
}
