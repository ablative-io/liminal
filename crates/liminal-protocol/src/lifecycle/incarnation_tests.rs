#![allow(clippy::expect_used, clippy::panic)]

use crate::{
    outcome::ConnectionIncarnationExhausted,
    wire::{BindingEpoch, ConnectionIncarnation, Generation},
};

use super::{
    ConnectionIncarnationAllocationDecision, ConnectionIncarnationAllocator,
    ConnectionIncarnationAllocatorRestore, ConnectionIncarnationAllocatorRestoreError,
    ConnectionOrdinalExhaustion, DurableIncarnationReferences, DurableIncarnationReferencesError,
    ServerIncarnationStartupDecision, allocate_connection_incarnation,
    prepare_server_incarnation_startup,
};

fn restore(
    server_incarnation: u64,
    last_examined_connection_ordinal: Option<u64>,
    connection_ordinal_exhausted: bool,
) -> ConnectionIncarnationAllocator {
    ConnectionIncarnationAllocator::try_restore(ConnectionIncarnationAllocatorRestore {
        server_incarnation,
        last_examined_connection_ordinal,
        connection_ordinal_exhausted,
    })
    .expect("test header is valid")
}

fn references(values: &[ConnectionIncarnation]) -> DurableIncarnationReferences<'_> {
    DurableIncarnationReferences::try_new(values, values.len())
        .expect("test reference set fits its bound")
}

#[test]
fn startup_checked_increment_is_a_deterministic_fsync_intent() {
    let persisted = ConnectionIncarnationAllocatorRestore {
        server_incarnation: 6,
        last_examined_connection_ordinal: Some(99),
        connection_ordinal_exhausted: false,
    };

    let first = prepare_server_incarnation_startup(
        ConnectionIncarnationAllocator::try_restore(persisted)
            .expect("persisted pre-state is valid"),
    );
    let replay = prepare_server_incarnation_startup(
        ConnectionIncarnationAllocator::try_restore(persisted)
            .expect("identical replay pre-state is valid"),
    );
    assert_eq!(first, replay);

    let ServerIncarnationStartupDecision::Fsync(intent) = first else {
        panic!("server value below MAX must checked-increment");
    };
    assert_eq!(intent.prior_server_incarnation(), 6);
    assert_eq!(intent.server_incarnation(), 7);
    assert_eq!(
        intent.header_to_fsync(),
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: None,
            connection_ordinal_exhausted: false,
        }
    );
    assert_eq!(
        intent.complete_after_fsync().as_restore(),
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: None,
            connection_ordinal_exhausted: false,
        }
    );
}

#[test]
fn case_42_server_max_returns_exact_unchanged_exhaustion() {
    let decision = prepare_server_incarnation_startup(restore(u64::MAX, Some(42), false));
    let ServerIncarnationStartupDecision::Exhausted(exhausted) = decision else {
        panic!("MAX server incarnation cannot wrap");
    };
    assert_eq!(
        exhausted.outcome(),
        ConnectionIncarnationExhausted::ServerIncarnation
    );
    assert_eq!(exhausted.outcome().current_value(), u64::MAX);
    assert_eq!(exhausted.outcome().attempted_server_incarnation(), None);
    assert_eq!(
        exhausted.into_unchanged().as_restore(),
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: u64::MAX,
            last_examined_connection_ordinal: Some(42),
            connection_ordinal_exhausted: false,
        }
    );
}

#[test]
fn fresh_server_allocates_zero_then_checked_increments() {
    let first = allocate_connection_incarnation(restore(7, None, false), references(&[]));
    let ConnectionIncarnationAllocationDecision::Allocated(first) = first else {
        panic!("fresh ordinal namespace must allocate zero");
    };
    assert_eq!(
        first.connection_incarnation(),
        ConnectionIncarnation::new(7, 0)
    );

    let second = allocate_connection_incarnation(first.into_resulting(), references(&[]));
    let ConnectionIncarnationAllocationDecision::Allocated(second) = second else {
        panic!("ordinal one remains available");
    };
    assert_eq!(
        second.connection_incarnation(),
        ConnectionIncarnation::new(7, 1)
    );
}

#[test]
fn case_42_non_max_collision_skips_before_publication() {
    let live = [
        ConnectionIncarnation::new(6, 10),
        ConnectionIncarnation::new(7, 10),
        ConnectionIncarnation::new(7, 11),
        ConnectionIncarnation::new(8, 12),
    ];
    let pre_state = ConnectionIncarnationAllocatorRestore {
        server_incarnation: 7,
        last_examined_connection_ordinal: Some(9),
        connection_ordinal_exhausted: false,
    };

    let decision = allocate_connection_incarnation(
        ConnectionIncarnationAllocator::try_restore(pre_state)
            .expect("collision pre-state is valid"),
        references(&live),
    );
    let replay = allocate_connection_incarnation(
        ConnectionIncarnationAllocator::try_restore(pre_state).expect("replay pre-state is valid"),
        references(&live),
    );
    assert_eq!(decision, replay);

    let ConnectionIncarnationAllocationDecision::Allocated(allocation) = decision else {
        panic!("ordinal twelve is collision-free");
    };
    assert_eq!(allocation.skipped_collisions(), 2);
    assert_eq!(
        allocation.connection_incarnation(),
        ConnectionIncarnation::new(7, 12)
    );
    assert_eq!(
        allocation.resulting_header(),
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: Some(12),
            connection_ordinal_exhausted: false,
        }
    );
}

#[test]
fn allocating_max_once_atomically_sets_exhaustion() {
    let decision =
        allocate_connection_incarnation(restore(7, Some(u64::MAX - 1), false), references(&[]));
    let ConnectionIncarnationAllocationDecision::Allocated(allocation) = decision else {
        panic!("unreferenced MAX may be allocated once");
    };
    assert_eq!(
        allocation.connection_incarnation(),
        ConnectionIncarnation::new(7, u64::MAX)
    );
    assert!(allocation.resulting_header().connection_ordinal_exhausted);

    let replay = allocate_connection_incarnation(allocation.into_resulting(), references(&[]));
    let ConnectionIncarnationAllocationDecision::Exhausted(exhausted) = replay else {
        panic!("MAX allocation permanently exhausts the ordinal namespace");
    };
    assert!(matches!(
        exhausted,
        ConnectionOrdinalExhaustion::AlreadyExhausted(_)
    ));
    assert_eq!(
        exhausted.outcome(),
        ConnectionIncarnationExhausted::ConnectionOrdinal {
            attempted_server_incarnation: 7,
        }
    );
    assert_eq!(exhausted.outcome().current_value(), u64::MAX);
    assert_eq!(exhausted.outcome().attempted_server_incarnation(), Some(7));
}

#[test]
fn referenced_max_sets_exhaustion_without_publishing_a_pair() {
    let live = [ConnectionIncarnation::new(7, u64::MAX)];
    let decision =
        allocate_connection_incarnation(restore(7, Some(u64::MAX - 1), false), references(&live));
    let ConnectionIncarnationAllocationDecision::Exhausted(exhausted) = decision else {
        panic!("referenced MAX cannot be published");
    };
    let ConnectionOrdinalExhaustion::MarkExhausted(commit) = &exhausted else {
        panic!("the first terminal collision must persist the exhaustion bit");
    };
    assert_eq!(commit.skipped_collisions(), 1);
    assert_eq!(
        commit.resulting_header(),
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: Some(u64::MAX),
            connection_ordinal_exhausted: true,
        }
    );
    assert_eq!(
        exhausted.outcome(),
        ConnectionIncarnationExhausted::ConnectionOrdinal {
            attempted_server_incarnation: 7,
        }
    );
}

#[test]
fn case_42_seeded_ordinal_exhaustion_is_idempotent() {
    let decision =
        allocate_connection_incarnation(restore(7, Some(u64::MAX), true), references(&[]));
    let ConnectionIncarnationAllocationDecision::Exhausted(exhausted) = decision else {
        panic!("exhausted seed has no candidate");
    };
    assert!(matches!(
        exhausted,
        ConnectionOrdinalExhaustion::AlreadyExhausted(_)
    ));
    assert_eq!(
        exhausted.resulting_header(),
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: Some(u64::MAX),
            connection_ordinal_exhausted: true,
        }
    );
    assert_eq!(exhausted.outcome().attempted_server_incarnation(), Some(7));
}

#[test]
fn restore_rejects_both_exhaustion_bit_mismatches() {
    for restored in [
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: Some(u64::MAX),
            connection_ordinal_exhausted: false,
        },
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: 7,
            last_examined_connection_ordinal: Some(4),
            connection_ordinal_exhausted: true,
        },
    ] {
        assert_eq!(
            ConnectionIncarnationAllocator::try_restore(restored),
            Err(
                ConnectionIncarnationAllocatorRestoreError::OrdinalExhaustionMismatch {
                    last_examined_connection_ordinal: restored.last_examined_connection_ordinal,
                    connection_ordinal_exhausted: restored.connection_ordinal_exhausted,
                }
            )
        );
    }
}

#[test]
fn complete_reference_collection_must_fit_its_declared_bound() {
    let live = [
        ConnectionIncarnation::new(7, 1),
        ConnectionIncarnation::new(7, 2),
    ];
    assert_eq!(
        DurableIncarnationReferences::try_new(&live, 1),
        Err(
            DurableIncarnationReferencesError::ReferenceCountExceedsBound {
                actual: 2,
                maximum: 1,
            }
        )
    );
}

#[test]
fn same_connection_rotation_changes_generation_not_incarnation() {
    let allocation = allocate_connection_incarnation(restore(7, None, false), references(&[]));
    let ConnectionIncarnationAllocationDecision::Allocated(allocation) = allocation else {
        panic!("fresh connection incarnation must allocate");
    };
    let connection_incarnation = allocation.connection_incarnation();
    let old_epoch = BindingEpoch::new(connection_incarnation, Generation::ONE);
    let new_generation = Generation::new(2).expect("generation two is nonzero");
    let rotated_epoch = BindingEpoch::new(connection_incarnation, new_generation);

    assert_ne!(old_epoch, rotated_epoch);
    assert_eq!(
        old_epoch.connection_incarnation,
        rotated_epoch.connection_incarnation
    );
}
