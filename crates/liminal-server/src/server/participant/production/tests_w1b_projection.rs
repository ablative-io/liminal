use std::error::Error;

use liminal_protocol::wire::{DetachedCause, DiedCause, ParticipantRecord};

use super::log::{
    StoredAttachAllocation, StoredAttachModeV3, StoredBindingEpoch, StoredComposedTerminal,
    StoredComposedTerminalCause, StoredComposedTerminalKind, StoredFencedAttachProof,
    StoredFinalizerPresentation, StoredU128,
};
use super::outbox_projection::project_attached_records;

const PARTICIPANT: u64 = 7;
const PRIOR: StoredBindingEpoch = StoredBindingEpoch {
    server_incarnation: 2,
    connection_ordinal: 3,
    capability_generation: 4,
};
const NEXT: StoredBindingEpoch = StoredBindingEpoch {
    server_incarnation: 2,
    connection_ordinal: 5,
    capability_generation: 5,
};

const fn allocation() -> StoredAttachAllocation {
    StoredAttachAllocation {
        binding_epoch: NEXT,
        attach_secret: [1; 32],
        attached_order: 20,
        attached_seq: 22,
        receipt_expires_at: StoredU128(23_u128.to_be_bytes()),
        provenance_expires_at: StoredU128(24_u128.to_be_bytes()),
        admitted_now_ms: 25,
    }
}

fn mode(
    kind: StoredComposedTerminalKind,
    cause: StoredComposedTerminalCause,
    presentation: StoredFinalizerPresentation,
) -> StoredAttachModeV3 {
    StoredAttachModeV3::Fenced {
        prior_binding_epoch: PRIOR,
        marker_delivery_seq: 11,
        marker_source_sequence: 2,
        proof: StoredFencedAttachProof {
            detached_credential_recovery: Vec::new(),
            predecessor_debt: Vec::new(),
            fenced_resulting_floor: 12,
            successor: Vec::new(),
        },
        composed_terminal: Some(StoredComposedTerminal {
            kind,
            cause,
            transaction_order: 20,
            delivery_seq: 21,
            pending_source_sequence: 3,
            presentation,
        }),
    }
}

#[test]
fn fenced_attached_projects_exact_presenting_terminal_then_attached() -> Result<(), Box<dyn Error>>
{
    let died = project_attached_records(
        PARTICIPANT,
        &allocation(),
        &mode(
            StoredComposedTerminalKind::Died,
            StoredComposedTerminalCause::ConnectionLost,
            StoredFinalizerPresentation::PresentEnclosing,
        ),
        None,
    )?;
    assert_eq!(died.len(), 2);
    assert_eq!(died[0].0, 21);
    assert!(matches!(
        &died[0].1,
        ParticipantRecord::Died {
            affected_participant_id: PARTICIPANT,
            cause: DiedCause::ConnectionLost,
            ..
        }
    ));
    assert_eq!(died[1].0, allocation().attached_seq);
    assert!(matches!(
        &died[1].1,
        ParticipantRecord::Attached {
            affected_participant_id: PARTICIPANT,
            ..
        }
    ));

    let detached = project_attached_records(
        PARTICIPANT,
        &allocation(),
        &mode(
            StoredComposedTerminalKind::Detached,
            StoredComposedTerminalCause::ServerShutdown,
            StoredFinalizerPresentation::PresentEnclosing,
        ),
        None,
    )?;
    assert!(matches!(
        &detached[0].1,
        ParticipantRecord::Detached {
            affected_participant_id: PARTICIPANT,
            cause: DetachedCause::ServerShutdown,
            ..
        }
    ));
    Ok(())
}

#[test]
fn recovered_reservation_consumer_projects_attached_without_terminal_witness()
-> Result<(), Box<dyn Error>> {
    let records = project_attached_records(
        PARTICIPANT,
        &allocation(),
        &mode(
            StoredComposedTerminalKind::Died,
            StoredComposedTerminalCause::ProtocolError,
            StoredFinalizerPresentation::ConsumeRecoveredReservation {
                recovered_source_sequence: 4,
            },
        ),
        None,
    )?;
    let [
        (
            delivery_seq,
            ParticipantRecord::Attached {
                affected_participant_id,
                ..
            },
        ),
    ] = records.as_slice()
    else {
        return Err("reservation consumer projected a terminal witness".into());
    };
    assert_eq!(*delivery_seq, allocation().attached_seq);
    assert_eq!(*affected_participant_id, PARTICIPANT);
    Ok(())
}
