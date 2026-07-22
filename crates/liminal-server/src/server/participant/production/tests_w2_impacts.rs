use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::algebra::WideResourceVector;
use liminal_protocol::lifecycle::{ClosureDebt, ClosureState, Event, ObserverProjection};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck, ParticipantRecord,
    ReceiptReplay, RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use crate::server::connection::ReadyWaker;
use crate::server::participant::dispatch_impact::{DispatchEffect, DispatchImpact, DispatchTarget};
use crate::server::participant::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantOfferedProgress, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::tests::test_participant_config;

const CONVERSATION: u64 = 0xF2_01;

fn installed_service() -> Result<InstalledParticipantService, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let config = test_participant_config();
    let handler: Arc<dyn ParticipantSemanticHandler> = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    InstalledParticipantService::new(handler, store, config.wire_frame_limit)
        .map_err(|error| format!("W2 impact fixture configuration failed: {error:?}").into())
}

fn apply(
    service: &InstalledParticipantService,
    incarnation: ConnectionIncarnation,
    conversations: &mut ParticipantConnectionConversations,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    service
        .handle(
            ParticipantConnectionContext::new(incarnation),
            conversations,
            request,
        )
        .map_err(Into::into)
}

#[test]
fn semantic_noop_refusal_and_unchanged_commit_emit_no_dispatch_tell() -> Result<(), Box<dyn Error>>
{
    let service = installed_service()?;
    let incarnation = ConnectionIncarnation::new(0xF2, 1);
    let token = EnrollmentToken::new([0x21; 16]);
    let request = EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: token,
    };
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = apply(
        &service,
        incarnation,
        &mut conversations,
        ClientRequest::Enrollment(request.clone()),
    )?;
    let ServerValue::EnrollBound(bound) = enrolled else {
        return Err(format!("W2 no-tell enrollment did not bind: {enrolled:?}").into());
    };

    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = service.new_publication_inbox();
    service.publication_registry().register(
        incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    assert!(!inbox.has_pending()?);

    let refusal = apply(
        &service,
        incarnation,
        &mut conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: u64::MAX,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x22; 16]),
            payload: Vec::new(),
        }),
    )?;
    assert!(matches!(refusal, ServerValue::ParticipantUnknown(_)));
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), u64::from(false));

    let replay = apply(
        &service,
        incarnation,
        &mut conversations,
        ClientRequest::Enrollment(request),
    )?;
    assert!(matches!(
        replay,
        ServerValue::Bound(ReceiptReplay::Enrollment(_))
    ));
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), u64::from(false));

    let no_op = apply(
        &service,
        incarnation,
        &mut conversations,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: bound.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 0,
        }),
    )?;
    assert!(matches!(no_op, ServerValue::AckNoOp(_)));
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), u64::from(false));
    Ok(())
}

#[test]
fn registration_is_passive_and_committed_bind_tells_without_sweep() -> Result<(), Box<dyn Error>> {
    let service = installed_service()?;
    let first_incarnation = ConnectionIncarnation::new(0xF2, 2);
    let rebound_incarnation = ConnectionIncarnation::new(0xF2, 3);
    let mut first_conversations = ParticipantConnectionConversations::default();
    let enrolled = apply(
        &service,
        first_incarnation,
        &mut first_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x23; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = enrolled else {
        return Err(format!("W2 passive-register enrollment did not bind: {enrolled:?}").into());
    };

    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = service.new_publication_inbox();
    service.publication_registry().register(
        rebound_incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    assert!(!inbox.has_pending()?);
    assert_eq!(wake_count.load(Ordering::SeqCst), u64::from(false));

    let mut rebound_conversations = ParticipantConnectionConversations::default();
    let rebound = apply(
        &service,
        rebound_incarnation,
        &mut rebound_conversations,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: bound.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x24; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(matches!(rebound, ServerValue::AttachBound(_)));
    assert_eq!(wake_count.load(Ordering::SeqCst), u64::from(true));
    let ready = inbox.take_ready()?;
    assert_eq!(ready.conversations, vec![CONVERSATION]);
    assert!(ready.observer_progressed.is_empty());
    Ok(())
}

fn two_enrollment_impact()
-> Result<(DispatchImpact, DispatchTarget, DispatchTarget), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let first_incarnation = ConnectionIncarnation::new(0xF2, 4);
    let second_incarnation = ConnectionIncarnation::new(0xF2, 5);
    let mut first_conversations = ParticipantConnectionConversations::default();
    let first = handler.handle_with_impact(
        ParticipantConnectionContext::new(first_incarnation),
        &mut first_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x25; 16]),
        }),
    );
    let (first_result, _) = first.into_parts();
    let ServerValue::EnrollBound(first_bound) = first_result? else {
        return Err("first multi-effect enrollment did not bind".into());
    };
    let mut second_conversations = ParticipantConnectionConversations::default();
    let second = handler.handle_with_impact(
        ParticipantConnectionContext::new(second_incarnation),
        &mut second_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x26; 16]),
        }),
    );
    let (second_result, impact) = second.into_parts();
    let ServerValue::EnrollBound(second_bound) = second_result? else {
        return Err("second multi-effect enrollment did not bind".into());
    };
    Ok((
        impact,
        DispatchTarget::new(
            first_bound.participant_id(),
            first_bound.origin_binding_epoch(),
        ),
        DispatchTarget::new(
            second_bound.participant_id(),
            second_bound.origin_binding_epoch(),
        ),
    ))
}

#[test]
fn dispatch_impact_unions_multi_effect_targets() -> Result<(), Box<dyn Error>> {
    let (impact, first, second) = two_enrollment_impact()?;
    let effects = impact
        .effects()
        .ok_or("committed overlapping enrollment returned Unchanged")?;
    assert_eq!(
        effects.get(&DispatchEffect::Published),
        Some(&BTreeSet::from([first]))
    );
    assert_eq!(
        effects.get(&DispatchEffect::BindingChanged),
        Some(&BTreeSet::from([second]))
    );
    assert_eq!(
        effects.get(&DispatchEffect::EpisodeChanged),
        Some(&BTreeSet::from([first, second]))
    );
    assert_eq!(impact.target_union(), BTreeSet::from([first, second]));
    Ok(())
}

#[test]
fn dispatch_impact_covers_state_preserving_marker_and_w1b_changes() -> Result<(), Box<dyn Error>> {
    let (impact, first, second) = two_enrollment_impact()?;
    let effects = impact
        .effects()
        .ok_or("committed binding/episode transition returned Unchanged")?;
    assert!(effects.contains_key(&DispatchEffect::Published));
    assert!(effects.contains_key(&DispatchEffect::BindingChanged));
    assert!(effects.contains_key(&DispatchEffect::EpisodeChanged));
    assert_eq!(impact.target_union(), BTreeSet::from([first, second]));

    for source in [
        include_str!("ops_acks.rs"),
        include_str!("ops_frontier.rs"),
        include_str!("ops_leave.rs"),
        include_str!("ops_session.rs"),
    ] {
        assert!(
            source.contains("record_") && source.contains("impact"),
            "committing operation lost its typed impact producer"
        );
    }
    assert!(include_str!("ops_acks.rs").contains("record_acknowledged"));
    assert!(include_str!("ops_acks.rs").contains("record_episode_changed"));
    let fate = include_str!("connection_fate_dispatch.rs");
    assert!(fate.contains("Some(&mut impact)"));
    assert!(fate.contains("complete_with_impact(authority, appender, impact)"));
    Ok(())
}

#[test]
fn debt_zero_transition_releases_deferred_obligation() -> Result<(), Box<dyn Error>> {
    let projection = ObserverProjection::new(1);
    let debt = ClosureDebt::new(WideResourceVector::new(1, 1))
        .ok_or("zero-transition fixture debt was zero")?;
    let event = Event::projection_completed(1);
    let successor = projection
        .clear_after_completion(&event)
        .ok_or("projection completion did not select Clear")?;
    let resulting = projection
        .complete(debt, event, successor)
        .map_err(|state| format!("Owed-to-Clear transition refused: {state:?}"))?;
    assert_eq!(resulting, ClosureState::Clear);

    let service = installed_service()?;
    let first_incarnation = ConnectionIncarnation::new(0xF2, 20);
    let second_incarnation = ConnectionIncarnation::new(0xF2, 21);
    let mut first_conversations = ParticipantConnectionConversations::default();
    let first = apply(
        &service,
        first_incarnation,
        &mut first_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x31; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(first) = first else {
        return Err("zero-transition wake fixture first enrollment did not bind".into());
    };

    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = service.new_publication_inbox();
    service.publication_registry().register(
        first_incarnation,
        &inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    assert!(!inbox.has_pending()?);

    let mut second_conversations = ParticipantConnectionConversations::default();
    let second = apply(
        &service,
        second_incarnation,
        &mut second_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x32; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(second) = second else {
        return Err("zero-transition wake fixture second enrollment did not bind".into());
    };
    assert_eq!(wake_count.load(Ordering::SeqCst), 1);
    let ready = inbox.take_ready()?;
    assert_eq!(ready.conversations, vec![CONVERSATION]);
    assert!(!inbox.has_pending()?);

    let released = service
        .next_publication(first_incarnation, CONVERSATION, None)?
        .ok_or("told transition did not release the least obligation")?;
    assert!(matches!(
        released.delivery.record,
        ParticipantRecord::Attached {
            affected_participant_id,
            ..
        } if affected_participant_id == second.participant_id()
    ));
    assert_eq!(
        service.next_publication(
            first_incarnation,
            CONVERSATION,
            Some(ParticipantOfferedProgress {
                binding_epoch: released.binding_epoch,
                through_seq: released.delivery_seq(),
            }),
        )?,
        None
    );
    assert_eq!(first.participant_id(), 0);
    Ok(())
}
