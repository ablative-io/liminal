//! THE CONTAINMENT GATE, RED BY DESIGN.
//!
//! Tom's bar, 2026-08-06: *"this whole thing has to be robust beyond belief...
//! the fact that something can kill something on that... constitutes an
//! absolute failure of the project."*
//!
//! The property that bar reduces to, mechanically:
//!
//! > No single unloadable durable record can prevent the node from starting,
//! > and no single unloadable conversation can take down the others. Refuse it,
//! > NAME it, keep serving.
//!
//! The property does not hold today, and it fails at one `?`:
//!
//! ```text
//! handler.rs:111 pub fn new -> :137 handler.restore_all_conversations()?;
//! handler.rs:237 restore_all_conversations -> :250 replay_and_repair(..)?
//! ```
//!
//! `:250` propagates a per-conversation replay failure out of a loop over
//! *every* conversation, and `:137` turns that into a constructor failure. One
//! poisoned conversation therefore takes the whole node down. The per-request
//! gate at `handler.rs:350` already behaves correctly — it fails exactly one
//! conversation and the node keeps serving — so the defect is the boot loop
//! alone.
//!
//! ATTRIBUTION IS PART OF THE SAME PROPERTY, NOT A SECOND TICKET. The four
//! sealed-binding-fate refusals (`ops_acks.rs:590`, `ops_nonzero_ack.rs:424`,
//! `binding_fate_completion.rs:83`, `connection_fate_replay.rs:155`) all name
//! the INVARIANT and never the SUBJECT: no conversation id appears in any of
//! them. Containment without attribution ships a node that boots clean and
//! silently serves nothing on one conversation forever — worse than the crash,
//! because the crash is the only thing currently telling anyone. So the gate
//! asserts three things, and the third is the one that stops it becoming a
//! silent-success test:
//!
//!   1. the node still BOOTS,
//!   2. the healthy conversation is still SERVED,
//!   3. the refusal NAMES the conversation it refused.
//!
//! The fault injected here is a genuinely unloadable durable record, not an I/O
//! outage: the bytes of one committed operation are replaced, so the row still
//! reads but no longer decodes. That is the shape of this week's failures —
//! intact storage, one record the replay cannot accept.
//!
//! These tests are EXPECTED TO FAIL until the containment change lands. They
//! are the red arm for it. Do not weaken the assertions to make them pass.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, ServerValue,
};

use crate::server::participant::ParticipantConnectionConversations;

use super::ProductionParticipantHandler;
use super::log::STREAM_PREFIX;
use super::tests::{dispatch_tracked, test_participant_config};

const POISONED_CONVERSATION: u64 = 7_201;
const HEALTHY_CONVERSATION: u64 = 7_202;
const POISONED_TOKEN: [u8; 16] = [0xC1; 16];
const HEALTHY_TOKEN: [u8; 16] = [0xC2; 16];

/// Replaces the payload of exactly one record on exactly one conversation's
/// operation stream, leaving every other byte in the store untouched.
///
/// This is "one durable record is unloadable" and nothing more: the stream is
/// present, its length is unchanged, its sequences are unchanged, and every
/// other conversation reads through verbatim.
#[derive(Debug)]
struct CorruptOneRecord {
    inner: Arc<dyn DurableStore>,
    target_key: String,
    target_sequence: u64,
}

impl CorruptOneRecord {
    fn new(inner: Arc<dyn DurableStore>, conversation_id: u64, target_sequence: u64) -> Self {
        Self {
            inner,
            target_key: format!("{STREAM_PREFIX}{conversation_id}"),
            target_sequence,
        }
    }

    fn corrupt(&self, stream_key: &str, mut entries: Vec<StoredEntry>) -> Vec<StoredEntry> {
        if stream_key != self.target_key {
            return entries;
        }
        for entry in &mut entries {
            if entry.sequence == self.target_sequence {
                entry.payload = vec![0xFF; 16];
            }
        }
        entries
    }
}

#[async_trait::async_trait]
impl DurableStore for CorruptOneRecord {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        self.inner.append(stream_key, payload, expected_seq).await
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        let entries = self.inner.read_from(stream_key, offset, limit).await?;
        Ok(self.corrupt(stream_key, entries))
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.inner.flush().await
    }
}

/// Seeds two independent conversations through the production request seam and
/// returns the shared store they were written to.
fn seed_two_conversations() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;

    for (conversation_id, token, incarnation) in [
        (POISONED_CONVERSATION, POISONED_TOKEN, 1_u64),
        (HEALTHY_CONVERSATION, HEALTHY_TOKEN, 2_u64),
    ] {
        let connection = ConnectionIncarnation::new(701, incarnation);
        let mut conversations = ParticipantConnectionConversations::default();
        let enrolled = dispatch_tracked(
            &handler,
            connection,
            &mut conversations,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new(token),
            }),
        )?;
        if !matches!(enrolled, ServerValue::EnrollBound(_)) {
            return Err(format!("seed enrollment did not bind: {enrolled:?}").into());
        }
    }

    drop(handler);
    Ok(store)
}

/// The control: the identical fixture with NOTHING corrupted.
///
/// Without this arm a failure below would prove only that the fixture cannot
/// boot, not that corruption is what stops it.
#[test]
fn control_uncorrupted_store_boots_and_serves_both_conversations() -> Result<(), Box<dyn Error>> {
    let store = seed_two_conversations()?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let ids = handler.registered_conversation_ids()?;
    assert!(
        ids.contains(&POISONED_CONVERSATION) && ids.contains(&HEALTHY_CONVERSATION),
        "control boot lost a conversation: {ids:?}"
    );
    Ok(())
}

/// ASSERTION 1 — THE NODE STILL BOOTS.
///
/// Fails today at `handler.rs:250`'s `?`, which climbs to `:137` inside
/// `ProductionParticipantHandler::new`.
#[test]
fn one_unloadable_record_must_not_prevent_the_node_from_starting()
-> Result<(), Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    let poisoned: Arc<dyn DurableStore> =
        Arc::new(CorruptOneRecord::new(inner, POISONED_CONVERSATION, 0));

    let outcome = ProductionParticipantHandler::new(poisoned, test_participant_config());

    assert!(
        outcome.is_ok(),
        "CONTAINMENT: one unloadable record on conversation {POISONED_CONVERSATION} took the \
         whole node down. The boot loop at handler.rs:250 propagated a per-conversation replay \
         failure out of a loop over every conversation. Error: {:?}",
        outcome.err()
    );
    Ok(())
}

/// ASSERTION 2 — THE OTHER CONVERSATIONS ARE STILL SERVED.
#[test]
fn one_unloadable_conversation_must_not_take_down_the_others() -> Result<(), Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    let poisoned: Arc<dyn DurableStore> =
        Arc::new(CorruptOneRecord::new(inner, POISONED_CONVERSATION, 0));

    let handler = ProductionParticipantHandler::new(poisoned, test_participant_config())
        .map_err(|error| format!("node did not boot (assertion 1 already covers this): {error}"))?;

    let connection = ConnectionIncarnation::new(702, 1);
    let mut conversations = ParticipantConnectionConversations::default();
    let served = dispatch_tracked(
        &handler,
        connection,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: HEALTHY_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xC3; 16]),
        }),
    )?;
    assert!(
        matches!(served, ServerValue::EnrollBound(_)),
        "CONTAINMENT: the healthy conversation {HEALTHY_CONVERSATION} stopped serving because \
         conversation {POISONED_CONVERSATION} is unloadable: {served:?}"
    );
    Ok(())
}

/// ASSERTION 3 — THE REFUSAL NAMES THE CONVERSATION IT REFUSED.
///
/// This is the assertion that stops containment becoming a silent-success
/// property. A node that boots clean and serves nothing on one conversation,
/// without ever naming it, is worse than the crash it replaced.
#[test]
fn the_refusal_must_name_the_conversation_it_refused() -> Result<(), Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    let poisoned: Arc<dyn DurableStore> =
        Arc::new(CorruptOneRecord::new(inner, POISONED_CONVERSATION, 0));

    let handler = ProductionParticipantHandler::new(Arc::clone(&poisoned), test_participant_config())
        .map_err(|error| format!("node did not boot (assertion 1 already covers this): {error}"))?;

    let connection = ConnectionIncarnation::new(703, 1);
    let mut conversations = ParticipantConnectionConversations::default();
    let refusal = dispatch_tracked(
        &handler,
        connection,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: POISONED_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xC4; 16]),
        }),
    );

    let error = match refusal {
        Ok(value) => {
            return Err(format!(
                "the poisoned conversation answered a request instead of refusing: {value:?}"
            )
            .into());
        }
        Err(error) => error.to_string(),
    };

    assert!(
        error.contains(&POISONED_CONVERSATION.to_string()),
        "ATTRIBUTION: the refusal names the invariant and not its subject. Nothing in this \
         message identifies conversation {POISONED_CONVERSATION}: {error}"
    );
    Ok(())
}
