//! Cold-replay regressions for server-owned binding-fate persistence.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::{DurableStore, HaematiteStore};
use liminal_protocol::lifecycle::{
    BindingState, DiedBindingTransition, ParticipantSlotAllocatorProof,
};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, DiedCause, EnrollmentRequest,
    EnrollmentToken, Generation,
};

use super::crash_repository::{
    CrashEnrollmentAllocation, CrashTerminalDisposition, ParticipantCrashCause,
    ParticipantCrashRepository,
};

#[derive(Clone, Copy, Debug)]
struct TestParticipantSlot {
    conversation_id: u64,
    participant_id: u64,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for TestParticipantSlot {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_id
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

#[derive(Clone, Copy, Debug)]
struct CrashCase {
    conversation_id: u64,
    participant_id: u64,
    server_incarnation: u64,
    cause: ParticipantCrashCause,
    expected_cause: DiedCause,
}

const CRASH_CASES: [CrashCase; 4] = [
    CrashCase {
        conversation_id: 81,
        participant_id: 1,
        server_incarnation: 31,
        cause: ParticipantCrashCause::ConnectionLost,
        expected_cause: DiedCause::ConnectionLost,
    },
    CrashCase {
        conversation_id: 82,
        participant_id: 2,
        server_incarnation: 32,
        cause: ParticipantCrashCause::ProcessKilled,
        expected_cause: DiedCause::ProcessKilled,
    },
    CrashCase {
        conversation_id: 83,
        participant_id: 3,
        server_incarnation: 33,
        cause: ParticipantCrashCause::ProtocolError,
        expected_cause: DiedCause::ProtocolError,
    },
    CrashCase {
        conversation_id: 84,
        participant_id: 4,
        server_incarnation: 34,
        cause: ParticipantCrashCause::UncleanServerRestart,
        expected_cause: DiedCause::UncleanServerRestart {
            prior_server_incarnation: 34,
        },
    },
];

fn create_store(
    data_dir: &std::path::Path,
) -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    let database = Database::create(DatabaseConfig {
        data_dir: data_dir.to_path_buf(),
        shard_count: 2,
        sweep_interval: None,
        distributed: None,
    })?;
    Ok(Arc::new(HaematiteStore::new(Arc::new(EventStore::new(
        database,
    )))))
}

fn reopen_store(
    data_dir: &std::path::Path,
) -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    let database = Database::open(data_dir)?;
    Ok(Arc::new(HaematiteStore::new(Arc::new(EventStore::new(
        database,
    )))))
}

fn epoch(case: CrashCase) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(case.server_incarnation, case.participant_id + 10),
        Generation::ONE,
    )
}

fn enrollment_allocation(case: CrashCase) -> CrashEnrollmentAllocation {
    CrashEnrollmentAllocation {
        attach_secret: AttachSecret::new([case.participant_id.to_le_bytes()[0]; 32]),
        origin_binding_epoch: epoch(case),
        attached_transaction_order: 1,
        attached_delivery_seq: 10,
        receipt_expires_at: 1_000,
        provenance_expires_at: 2_000,
        enrollment_fingerprint: [case.conversation_id.to_le_bytes()[0]; 32],
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn all_crash_causes_survive_a_cold_database_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let data_dir = directory.path().join("participant-crash-db");

    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let store = create_store(&data_dir)?;
        for case in CRASH_CASES {
            let repository =
                ParticipantCrashRepository::new(Arc::clone(&store), case.conversation_id);
            let enrolled = runtime.block_on(repository.commit_enrollment(
                EnrollmentRequest {
                    conversation_id: case.conversation_id,
                    enrollment_token: EnrollmentToken::new(
                        [case.participant_id.to_le_bytes()[0]; 16],
                    ),
                },
                TestParticipantSlot {
                    conversation_id: case.conversation_id,
                    participant_id: case.participant_id,
                    identity_limit: 16,
                },
                enrollment_allocation(case),
            ))?;
            assert_eq!(enrolled.participant_id(), case.participant_id);

            let transition = runtime.block_on(repository.commit_crash(
                case.cause,
                CrashTerminalDisposition::Committed {
                    transaction_order: 2,
                    delivery_seq: 11,
                },
            ))?;
            let DiedBindingTransition::Committed(terminal) = transition else {
                panic!("committed placement must produce a committed Died terminal");
            };
            assert_eq!(terminal.participant_id(), case.participant_id);
            assert_eq!(terminal.conversation_id(), case.conversation_id);
            assert_eq!(terminal.binding_epoch(), epoch(case));
            assert_eq!(terminal.cause(), case.expected_cause);
            assert_eq!(terminal.delivery_seq(), 11);

            let entries = runtime.block_on(store.read_from(repository.stream_key(), 0, 8))?;
            assert_eq!(entries.len(), 2, "enrollment and crash each append once");
            assert_eq!(entries[0].sequence, 0);
            assert_eq!(entries[1].sequence, 1);
        }
        runtime.block_on(store.flush())?;
        drop(store);
        drop(runtime);
    }

    // All writer-side repository, store, EventStore, Database, and runtime
    // handles are gone before the same physical database path is reopened.
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let store = reopen_store(&data_dir)?;
        for case in CRASH_CASES {
            let repository =
                ParticipantCrashRepository::new(Arc::clone(&store), case.conversation_id);
            let recovered = runtime
                .block_on(repository.recover())?
                .ok_or("crash stream must recover a participant")?;
            assert_eq!(recovered.binding_state(), BindingState::Detached);

            let terminal = recovered
                .member()
                .latest_terminal()
                .ok_or("committed crash must be retained in member history")?;
            assert_eq!(terminal.participant_id(), case.participant_id);
            assert_eq!(terminal.conversation_id(), case.conversation_id);
            assert_eq!(terminal.binding_epoch(), epoch(case));
            assert_eq!(terminal.died_cause(), Some(case.expected_cause));
            assert_eq!(terminal.delivery_seq(), 11);

            let Some(DiedBindingTransition::Committed(replayed_terminal)) =
                recovered.last_transition()
            else {
                panic!("cold replay must reproduce the committed crash transition");
            };
            assert_eq!(
                replayed_terminal.participant_id(),
                terminal.participant_id()
            );
            assert_eq!(
                replayed_terminal.conversation_id(),
                terminal.conversation_id()
            );
            assert_eq!(replayed_terminal.binding_epoch(), terminal.binding_epoch());
            assert_eq!(replayed_terminal.delivery_seq(), terminal.delivery_seq());
            assert_eq!(replayed_terminal.cause(), case.expected_cause);
        }
    }

    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn pending_crash_replays_private_pending_authority_without_a_terminal_history_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let data_dir = directory.path().join("pending-participant-crash-db");
    let case = CrashCase {
        conversation_id: 91,
        participant_id: 7,
        server_incarnation: 41,
        cause: ParticipantCrashCause::ProtocolError,
        expected_cause: DiedCause::ProtocolError,
    };

    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let store = create_store(&data_dir)?;
        let repository = ParticipantCrashRepository::new(Arc::clone(&store), case.conversation_id);
        runtime.block_on(repository.commit_enrollment(
            EnrollmentRequest {
                conversation_id: case.conversation_id,
                enrollment_token: EnrollmentToken::new([0x91; 16]),
            },
            TestParticipantSlot {
                conversation_id: case.conversation_id,
                participant_id: case.participant_id,
                identity_limit: 16,
            },
            enrollment_allocation(case),
        ))?;
        let transition = runtime.block_on(repository.commit_crash(
            case.cause,
            CrashTerminalDisposition::Pending {
                transaction_order: 8,
            },
        ))?;
        let DiedBindingTransition::Pending(pending) = transition else {
            panic!("pending placement must produce pending Died authority");
        };
        assert_eq!(pending.participant_id(), case.participant_id);
        assert_eq!(pending.binding_epoch(), epoch(case));
        assert_eq!(pending.cause(), DiedCause::ProtocolError);
        assert_eq!(pending.admission_order().transaction_order(), 8);
        runtime.block_on(repository.flush())?;
        drop(repository);
        drop(store);
        drop(runtime);
    }

    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let store = reopen_store(&data_dir)?;
        let repository = ParticipantCrashRepository::new(store, case.conversation_id);
        let recovered = runtime
            .block_on(repository.recover())?
            .ok_or("pending crash stream must recover a participant")?;
        assert_eq!(recovered.member().latest_terminal(), None);
        let BindingState::PendingFinalization(pending) = recovered.binding_state() else {
            panic!("pending crash must cold-replay the private pending state");
        };
        assert_eq!(pending.participant_id(), case.participant_id);
        assert_eq!(pending.binding_epoch(), epoch(case));
        assert_eq!(pending.died_cause(), Some(DiedCause::ProtocolError));
        assert_eq!(pending.admission_order().transaction_order(), 8);
    }

    Ok(())
}
