use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use liminal::protocol::Frame;

use super::{
    FixtureSource, INCARNATION, RecordingSink, decode_push, service,
    service_participant_publications, state,
};
use crate::server::connection::ReadyWaker;
use crate::server::connection::delivery::DeliverySink;
use crate::server::connection::outbound::OutboundError;
use crate::server::participant::{
    ObserverPublication, ObserverPublicationTarget, ParticipantPublicationError,
    ParticipantPublicationInbox,
};

#[derive(Debug)]
struct PublishingSink {
    inner: RecordingSink,
    publish_on_next_enqueue: Option<(ObserverPublicationTarget, ObserverPublication)>,
}

impl PublishingSink {
    fn new(
        capacity: usize,
        target: ObserverPublicationTarget,
        publication: ObserverPublication,
    ) -> Self {
        Self {
            inner: RecordingSink::new(capacity),
            publish_on_next_enqueue: Some((target, publication)),
        }
    }
}

impl DeliverySink for PublishingSink {
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    fn has_room(&self, needed: usize) -> bool {
        self.inner.has_room(needed)
    }

    fn enqueue_frame(&mut self, frame: &Frame) -> Result<(), OutboundError> {
        self.inner.enqueue_frame(frame)?;
        if let Some((target, publication)) = self.publish_on_next_enqueue.take() {
            assert!(matches!(target.publish(publication), Ok(true)));
        }
        Ok(())
    }
}

#[test]
fn budget_requeue_keeps_newer_observer_wake() -> Result<(), Box<dyn std::error::Error>> {
    const TRIGGER_CONVERSATION: u64 = 1;
    const OBSERVED_CONVERSATION: u64 = 2;

    let source = Arc::new(FixtureSource::new([]));
    let service = service(source)?;
    let mut state = state(&service, &[])?;
    let wake_count = Arc::new(AtomicU64::new(0));
    let inbox = state
        .participant_publication
        .as_ref()
        .ok_or("missing publication inbox")?;
    service.publication_registry().register(
        INCARNATION,
        inbox,
        ReadyWaker::for_test(Arc::clone(&wake_count)),
    )?;
    let target = service
        .publication_registry()
        .observer_target(INCARNATION)?
        .ok_or("missing observer publication target")?;
    let trigger = ObserverPublication {
        conversation_id: TRIGGER_CONVERSATION,
        refused_epoch: 0,
        observer_progress: 1,
    };
    let older = ObserverPublication {
        conversation_id: OBSERVED_CONVERSATION,
        refused_epoch: 0,
        observer_progress: 1,
    };
    let newer = ObserverPublication {
        conversation_id: OBSERVED_CONVERSATION,
        refused_epoch: 1,
        observer_progress: 2,
    };

    assert!(target.publish(older)?);
    assert!(target.publish(trigger)?);
    assert_eq!(
        wake_count.load(Ordering::SeqCst),
        1,
        "coalesced initial work has one READY edge"
    );

    // The lower conversation is serviced first. Its enqueue callback fires the
    // newer payload after the pump took `older`, but before budget exhaustion
    // requeues that deferred payload.
    let mut sink = PublishingSink::new(4096, target, newer);
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        1
    );
    assert_eq!(
        decode_push(&sink.inner.frames[0])?,
        trigger.into_server_push()
    );
    assert_eq!(
        wake_count.load(Ordering::SeqCst),
        2,
        "newer post-take publication emits one edge and requeue emits none"
    );

    // The next slice must deliver only the incumbent newer payload. A final
    // empty slice proves the deferred older payload neither survived nor
    // duplicated delivery.
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        1
    );
    assert_eq!(
        decode_push(&sink.inner.frames[1])?,
        newer.into_server_push()
    );
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 1)?,
        0
    );
    assert_eq!(sink.inner.frames.len(), 2);
    assert_eq!(wake_count.load(Ordering::SeqCst), 2);
    assert!(
        !state
            .participant_publication
            .as_ref()
            .ok_or("missing publication inbox")?
            .has_pending()?
    );

    // A vacant key still accepts a deferred payload.
    let bounded = ParticipantPublicationInbox::new(1);
    bounded.requeue_observers([older])?;
    let vacant = bounded.take_ready()?;
    assert!(vacant.conversations.is_empty());
    assert_eq!(vacant.observer_progressed, vec![older]);

    // The same signed bound still refuses a different conversation with the
    // typed capacity error, and refusal leaves the incumbent intact.
    bounded.requeue_observers([trigger])?;
    let Err(refusal) = bounded.requeue_observers([newer]) else {
        return Err("a vacant second conversation did not exceed the signed bound".into());
    };
    assert!(matches!(
        refusal,
        ParticipantPublicationError::InboxCapacity { limit: 1 }
    ));
    let retained = bounded.take_ready()?;
    assert_eq!(retained.observer_progressed, vec![trigger]);
    Ok(())
}
