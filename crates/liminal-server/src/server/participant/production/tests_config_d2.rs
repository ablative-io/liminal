//! Signed D2 consumers, accumulated validation, and canonical-size riders.

use std::error::Error;

use liminal_protocol::lifecycle::{
    AdmissionOrder, BindingTerminalOwner, FrontierBinding, ImmutableSequenceCandidate,
    MarkerCandidateAuthority, MarkerProvenance, MarkerSequenceOwner, TerminalProductSource,
};
use liminal_protocol::outcome::CandidatePhase;
use liminal_protocol::wire::{BindingEpoch, ConnectionIncarnation, Generation};

use super::frontier::{attached_charge, recovery_row_sizes};
use super::log::{StoredBindingEpoch, StoredEnrollmentAllocation, StoredU128};
use super::ops_frontier::canonical_marker_bytes;
use super::tests::test_participant_config;

#[test]
fn d2_validation_accumulates_every_field_specific_failure() {
    let mut config = test_participant_config();
    config.max_ordinary_record_entries = 0;
    config.max_ordinary_record_bytes = 0;
    config.max_generated_marker_entries = 0;
    config.max_generated_marker_bytes = 0;
    config.mandatory_transaction_bound_entries = 0;
    config.mandatory_transaction_bound_bytes = 0;
    config.full_recovery_claim_entries = 1;
    config.full_recovery_claim_bytes = 1;
    config.retained_capacity_entries = 0;
    config.retained_capacity_bytes = 0;
    config.max_retained_record_rows = 0;
    config.closure_episode_churn_limit = 1;

    let mut errors = Vec::new();
    config.collect_errors(&mut errors);
    let joined = errors.join("\n");
    for field in [
        "max_ordinary_record_entries",
        "max_ordinary_record_bytes",
        "max_generated_marker_entries",
        "max_generated_marker_bytes",
        "mandatory_transaction_bound_entries",
        "mandatory_transaction_bound_bytes",
        "full_recovery_claim_entries",
        "full_recovery_claim_bytes",
        "retained_capacity_entries",
        "retained_capacity_bytes",
        "max_retained_record_rows",
        "closure_episode_churn_limit",
    ] {
        assert!(joined.contains(field), "missing D2 validation for {field}");
    }
}

#[test]
fn canonical_v2_marker_attached_and_recovery_rows_fit_signed_caps() -> Result<(), Box<dyn Error>> {
    let config = test_participant_config();
    let epoch = BindingEpoch::new(
        ConnectionIncarnation::new(u64::MAX, u64::MAX),
        Generation::ONE,
    );
    let terminal = BindingTerminalOwner {
        participant_index: u64::MAX,
        binding_epoch: epoch,
    };
    let provenances = [
        MarkerProvenance::NonProductM,
        MarkerProvenance::terminal_product(TerminalProductSource::Binding(terminal), u64::MAX),
        MarkerProvenance::terminal_product(
            TerminalProductSource::recovery_replacement(u64::MAX, epoch),
            u64::MAX,
        ),
        MarkerProvenance::exit_product(u64::MAX - 1, u64::MAX),
    ];
    let targets = [
        FrontierBinding::Bound(epoch),
        FrontierBinding::Detached(epoch),
    ];
    let mut marker_sizes = Vec::new();
    for target_binding in targets {
        for provenance in provenances {
            let candidate = ImmutableSequenceCandidate::Marker(MarkerCandidateAuthority {
                delivery_seq: u64::MAX,
                admission_order: AdmissionOrder::new(
                    u64::MAX,
                    CandidatePhase::CompactionMarker,
                    u64::MAX,
                ),
                target_binding,
                provenance,
                current_owner: MarkerSequenceOwner::Marker,
            });
            let size = u64::try_from(canonical_marker_bytes(candidate)?.len())?;
            assert!(
                size <= config.max_generated_marker_bytes,
                "canonical v2 marker row is {size} bytes, signed cap is {}",
                config.max_generated_marker_bytes
            );
            marker_sizes.push(size);
        }
    }

    let allocation = StoredEnrollmentAllocation {
        participant_id: u64::MAX,
        identity_limit: u64::MAX,
        attach_secret: [u8::MAX; 32],
        origin_epoch: epoch.into(),
        attached_order: u64::MAX,
        attached_seq: u64::MAX,
        receipt_expires_at: StoredU128::from(u128::MAX),
        provenance_expires_at: StoredU128::from(u128::MAX),
        enrollment_fingerprint: [u8::MAX; 32],
    };
    let attached_size = attached_charge(u64::MAX, &allocation)?.bytes;
    assert!(
        attached_size <= config.mandatory_transaction_bound_bytes,
        "canonical v2 Attached row is {attached_size} bytes, signed cap is {}",
        config.mandatory_transaction_bound_bytes
    );

    let (recovery_sequence_size, recovery_terminal_size) =
        recovery_row_sizes(u64::MAX, u64::MAX, epoch)?;
    for (name, size) in [
        ("RS", recovery_sequence_size),
        ("RT", recovery_terminal_size),
    ] {
        assert!(
            size <= config.full_recovery_claim_bytes,
            "canonical v2 {name} row is {size} bytes, signed cap is {}",
            config.full_recovery_claim_bytes
        );
    }

    eprintln!(
        "measured canonical v2 bytes: marker variants={marker_sizes:?}; Attached={attached_size}; RS={recovery_sequence_size}; RT={recovery_terminal_size}"
    );
    assert_eq!(StoredBindingEpoch::from(epoch), allocation.origin_epoch);
    Ok(())
}
