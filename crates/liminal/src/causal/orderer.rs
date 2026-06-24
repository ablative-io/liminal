use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tracing::warn;

use crate::causal::MessageId;
use crate::envelope::Envelope;

/// Envelope emitted by the causal orderer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderedEnvelope {
    /// Envelope ready for downstream delivery.
    pub envelope: Envelope,
    /// True when the envelope was released because its parent did not arrive before timeout.
    pub orphaned: bool,
}

impl OrderedEnvelope {
    /// Creates an ordered envelope result.
    #[must_use]
    pub const fn new(envelope: Envelope, orphaned: bool) -> Self {
        Self { envelope, orphaned }
    }

    /// Returns the message id carried by the emitted envelope.
    #[must_use]
    pub const fn message_id(&self) -> MessageId {
        self.envelope.message_id
    }
}

#[derive(Clone, Debug)]
struct BufferedEnvelope {
    envelope: Envelope,
    buffered_at: Instant,
}

impl BufferedEnvelope {
    fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            buffered_at: Instant::now(),
        }
    }

    fn is_expired(&self, now: Instant, orphan_timeout: Duration) -> bool {
        now.saturating_duration_since(self.buffered_at) >= orphan_timeout
    }
}

/// In-memory causal-chain orderer for envelopes.
#[derive(Debug)]
pub struct CausalOrderer {
    emitted: HashSet<MessageId>,
    buffered_by_parent: HashMap<MessageId, Vec<BufferedEnvelope>>,
    orphan_timeout: Duration,
}

impl CausalOrderer {
    /// Creates an orderer with the timeout used to release orphaned children.
    #[must_use]
    pub fn new(orphan_timeout: Duration) -> Self {
        Self {
            emitted: HashSet::new(),
            buffered_by_parent: HashMap::new(),
            orphan_timeout,
        }
    }

    /// Submits an envelope and returns every envelope ready for causal-order delivery.
    ///
    /// Independent envelopes and children whose parent has already emitted pass through
    /// immediately. Children whose direct parent has not emitted yet are buffered until
    /// that parent is submitted or the orphan timeout is drained.
    pub fn submit(&mut self, envelope: Envelope) -> Vec<OrderedEnvelope> {
        match direct_parent(&envelope) {
            Some(parent) if !self.emitted.contains(&parent) => {
                self.buffered_by_parent
                    .entry(parent)
                    .or_default()
                    .push(BufferedEnvelope::new(envelope));
                Vec::new()
            }
            Some(_) | None => {
                let mut ready = Vec::new();
                self.emit_ready(envelope, false, &mut ready);
                ready
            }
        }
    }

    /// Emits buffered messages whose parent has not arrived within the configured timeout.
    ///
    /// Each expired envelope is returned with [`OrderedEnvelope::orphaned`] set and is also
    /// recorded as emitted, allowing its own buffered children to proceed.
    pub fn drain_orphans(&mut self) -> Vec<OrderedEnvelope> {
        let now = Instant::now();
        let mut expired = Vec::new();
        let buffered_message_ids = self.buffered_message_ids();

        self.buffered_by_parent.retain(|parent, buffered| {
            let mut pending = Vec::with_capacity(buffered.len());
            for item in std::mem::take(buffered) {
                if item.is_expired(now, self.orphan_timeout)
                    && !buffered_message_ids.contains(parent)
                {
                    expired.push((*parent, item));
                } else {
                    pending.push(item);
                }
            }
            *buffered = pending;
            !buffered.is_empty()
        });

        let mut ready = Vec::new();
        for (missing_parent, item) in expired {
            warn!(
                missing_parent = ?missing_parent,
                message_id = ?item.envelope.message_id,
                "emitting orphaned causal message after parent timeout"
            );
            self.emit_ready(item.envelope, true, &mut ready);
        }
        ready
    }

    fn buffered_message_ids(&self) -> HashSet<MessageId> {
        self.buffered_by_parent
            .values()
            .flat_map(|children| children.iter().map(|child| child.envelope.message_id))
            .collect()
    }

    fn emit_ready(&mut self, envelope: Envelope, orphaned: bool, ready: &mut Vec<OrderedEnvelope>) {
        let message_id = envelope.message_id;
        self.emitted.insert(message_id);
        ready.push(OrderedEnvelope::new(envelope, orphaned));
        self.drain_children(message_id, ready);
    }

    fn drain_children(&mut self, parent: MessageId, ready: &mut Vec<OrderedEnvelope>) {
        if let Some(children) = self.buffered_by_parent.remove(&parent) {
            for child in children {
                self.emit_ready(child.envelope, false, ready);
            }
        }
    }
}

fn direct_parent(envelope: &Envelope) -> Option<MessageId> {
    envelope
        .causal_context
        .as_ref()
        .and_then(|context| context.parent)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::CausalOrderer;
    use crate::causal::{CausalContext, MessageId};
    use crate::channel::SchemaId;
    use crate::envelope::{Envelope, PublisherId};

    #[test]
    fn buffers_child_until_parent_emits() {
        let parent_id = MessageId::new();
        let child_id = MessageId::new();
        let mut orderer = CausalOrderer::new(Duration::from_secs(60));

        let child = envelope(child_id, Some(CausalContext::child_of(parent_id)));
        assert!(orderer.submit(child).is_empty());

        let parent = envelope(parent_id, None);
        let emitted = orderer.submit(parent);

        assert_eq!(message_ids(&emitted), vec![parent_id, child_id]);
        assert!(emitted.iter().all(|entry| !entry.orphaned));
    }

    #[test]
    fn independent_messages_emit_immediately() {
        let message_id = MessageId::new();
        let mut orderer = CausalOrderer::new(Duration::from_secs(60));

        let emitted = orderer.submit(envelope(message_id, None));

        assert_eq!(message_ids(&emitted), vec![message_id]);
        assert!(!emitted[0].orphaned);
    }

    #[test]
    fn orphan_timeout_emits_with_warning_flag() {
        let parent_id = MessageId::new();
        let child_id = MessageId::new();
        let mut orderer = CausalOrderer::new(Duration::ZERO);

        let child = envelope(child_id, Some(CausalContext::child_of(parent_id)));
        assert!(orderer.submit(child).is_empty());

        let emitted = orderer.drain_orphans();

        assert_eq!(message_ids(&emitted), vec![child_id]);
        assert!(emitted[0].orphaned);
    }

    #[test]
    fn orphan_drain_keeps_buffered_chain_in_causal_order() {
        let missing_root_id = MessageId::new();
        let parent_id = MessageId::new();
        let child_id = MessageId::new();
        let mut orderer = CausalOrderer::new(Duration::ZERO);

        let child = envelope(child_id, Some(CausalContext::child_of(parent_id)));
        assert!(orderer.submit(child).is_empty());

        let parent = envelope(parent_id, Some(CausalContext::child_of(missing_root_id)));
        assert!(orderer.submit(parent).is_empty());

        let emitted = orderer.drain_orphans();

        assert_eq!(message_ids(&emitted), vec![parent_id, child_id]);
        assert!(emitted[0].orphaned);
        assert!(!emitted[1].orphaned);
    }

    fn envelope(message_id: MessageId, causal_context: Option<CausalContext>) -> Envelope {
        Envelope::with_message_id(
            message_id,
            b"{}".to_vec(),
            causal_context,
            SchemaId::new(),
            PublisherId::from("causal-orderer-test"),
        )
    }

    fn message_ids(emitted: &[crate::causal::OrderedEnvelope]) -> Vec<MessageId> {
        emitted
            .iter()
            .map(crate::causal::OrderedEnvelope::message_id)
            .collect()
    }
}
