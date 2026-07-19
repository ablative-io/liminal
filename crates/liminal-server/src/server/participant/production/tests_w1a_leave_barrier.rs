//! Real-handler Left-before-Advance durability ordering oracle.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest, ServerValue,
};

use crate::server::participant::ParticipantConnectionConversations;

use super::tests_observer_wake::{apply, installed, observer_advance_rows, recover, register};
use super::tests_observer_wake_fixture::{BarrierKind, ObserverBarrierStore};

fn assert_advance_failure_prefix(
    service: &Arc<crate::server::participant::InstalledParticipantService>,
    barriers: &Arc<ObserverBarrierStore>,
    store: &Arc<dyn DurableStore>,
) -> Result<(), Box<dyn Error>> {
    let conversation_id = 7_702;
    let incarnation = ConnectionIncarnation::new(0x77, 3);
    let enrolled = apply(
        service,
        incarnation,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x73; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err("failure-prefix Leave fixture did not enroll".into());
    };
    let observer_incarnation = ConnectionIncarnation::new(0x77, 4);
    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = register(service, observer_incarnation, Arc::clone(&wake_count))?;
    assert!(
        recover(
            service,
            observer_incarnation,
            &mut ParticipantConnectionConversations::default(),
            conversation_id,
            0,
        )?
        .armed
    );
    let advances_before = observer_advance_rows(store)?;
    barriers.fail_next(BarrierKind::Advance)?;
    let result = apply(
        service,
        incarnation,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0x74; 16]),
        }),
    );
    let Err(error) = result else {
        return Err("observer Advance flush failure was accepted".into());
    };
    if !format!("{error:?}").contains("sequence conflict") {
        return Err(format!("unexpected observer durability failure: {error:?}").into());
    }
    assert_eq!(
        observer_advance_rows(store)?,
        advances_before + 1,
        "failed flush did not retain the exact appended observer prefix"
    );
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn leave_source_flush_precedes_observer_advance_flush() -> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let barriers = Arc::new(ObserverBarrierStore::new(inner));
    let store: Arc<dyn DurableStore> = barriers.clone();
    let service = Arc::new(installed(Arc::clone(&store))?);
    let conversation_id = 7_701;
    let leaver_incarnation = ConnectionIncarnation::new(0x77, 1);
    let enrolled = apply(
        &service,
        leaver_incarnation,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x71; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err("Leave barrier fixture did not enroll".into());
    };

    let observer_incarnation = ConnectionIncarnation::new(0x77, 2);
    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = register(&service, observer_incarnation, Arc::clone(&wake_count))?;
    let mut observer_conversations = ParticipantConnectionConversations::default();
    assert!(
        recover(
            &service,
            observer_incarnation,
            &mut observer_conversations,
            conversation_id,
            0,
        )?
        .armed
    );
    let advances_before = observer_advance_rows(&store)?;
    barriers.arm([BarrierKind::Source, BarrierKind::Advance])?;
    let leave_service = Arc::clone(&service);
    let leave = LeaveRequest {
        conversation_id,
        participant_id: receipt.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: receipt.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0x72; 16]),
    };
    let leave_thread = std::thread::spawn(move || {
        apply(
            &leave_service,
            leaver_incarnation,
            &mut ParticipantConnectionConversations::default(),
            ClientRequest::Leave(leave),
        )
        .map_err(|error| error.to_string())
    });

    barriers.wait_for(BarrierKind::Source)?;
    assert_eq!(observer_advance_rows(&store)?, advances_before);
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    barriers.release(BarrierKind::Source)?;
    barriers.wait_for(BarrierKind::Advance)?;
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    barriers.release(BarrierKind::Advance)?;
    let result = leave_thread
        .join()
        .map_err(|_| "Leave barrier thread panicked")??;
    assert!(matches!(result, ServerValue::LeaveCommitted(_)));
    assert!(inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), 1);
    assert_eq!(observer_advance_rows(&store)?, advances_before + 1);
    assert_advance_failure_prefix(&service, &barriers, &store)?;
    Ok(())
}
