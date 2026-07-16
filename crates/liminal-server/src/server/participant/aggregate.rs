//! Durable owner for one opaque participant-conversation aggregate.
//!
//! The server stores only canonical protocol events. It never serializes the
//! private lifecycle state: cold load starts from protocol genesis and consumes
//! each decoded event through protocol replay. A live transition remains inside
//! [`ConversationCommit`] until its exact bytes append successfully.

use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal_protocol::lifecycle::{
    ConversationDecision, ConversationEvent, ConversationEventDecodeError, ConversationGenesis,
    ConversationRefusalReason, ConversationReplayError, ParticipantConversation,
};

use super::conversation_stream::{ConversationEventStream, ConversationStreamError};

/// Failure while cold-loading or initializing one participant conversation.
#[derive(Debug, thiserror::Error)]
pub(super) enum ConversationAggregateError {
    /// The append-only conversation stream rejected a read or append.
    #[error(transparent)]
    Stream(#[from] ConversationStreamError),
    /// Stored event bytes are not one canonical protocol event.
    #[error("conversation event {stored_sequence} failed canonical decode: {reason:?}")]
    EventDecode {
        /// Durable stream position carrying the malformed bytes.
        stored_sequence: u64,
        /// Stable protocol codec failure.
        reason: ConversationEventDecodeError,
    },
    /// The event's canonical ordinal differs from its durable stream position.
    #[error(
        "conversation event ordinal mismatch: stored at {stored_sequence}, event names {event_ordinal}"
    )]
    StoredEventOrdinal {
        /// Durable stream position.
        stored_sequence: u64,
        /// Ordinal decoded from the canonical event.
        event_ordinal: u64,
    },
    /// Protocol replay rejected the structurally valid durable event.
    #[error("protocol rejected conversation event {stored_sequence}: {reason:?}")]
    Replay {
        /// Durable stream position of the rejected event.
        stored_sequence: u64,
        /// Exact protocol replay failure.
        reason: ConversationReplayError,
    },
    /// A prepared protocol event did not own the current durable stream head.
    #[error(
        "prepared conversation event ordinal mismatch: stream head {stream_head}, event ordinal {event_ordinal}"
    )]
    PreparedEventOrdinal {
        /// Exact optimistic stream head.
        stream_head: u64,
        /// Protocol-emitted event ordinal.
        event_ordinal: u64,
    },
    /// An uninitialized aggregate returned a non-commit protocol decision.
    #[error("protocol refused required genesis validation: {reason:?}")]
    GenesisRefused {
        /// Exact protocol refusal.
        reason: ConversationRefusalReason,
    },
    /// Append failed and the mandatory immediate cold reload also failed.
    #[error("genesis append failed ({append}); cold reload then failed ({reload})")]
    ReloadAfterAppendFailure {
        /// Original append failure.
        append: ConversationStreamError,
        /// Failure reconstructing durable reality after the ambiguous append.
        reload: Box<Self>,
    },
}

/// Result of opening one participant conversation stream.
#[derive(Debug)]
pub(super) enum ConversationAggregateOpen {
    /// Cold replay and any required genesis append completed durably.
    Ready(ParticipantConversationAggregate),
    /// Genesis append failed; the speculative commit was aborted and durable
    /// reality was cold-reloaded without retrying the transition.
    AppendFailed(ConversationAggregateAppendFailure),
}

/// Failed genesis append paired with its freshly reloaded aggregate.
#[derive(Debug)]
pub(super) struct ConversationAggregateAppendFailure {
    error: ConversationStreamError,
    reloaded: ParticipantConversationAggregate,
}

impl ConversationAggregateAppendFailure {
    /// Returns the original non-retried stream append failure.
    #[must_use]
    pub(super) const fn error(&self) -> &ConversationStreamError {
        &self.error
    }

    /// Borrows the aggregate reconstructed from durable storage after failure.
    #[must_use]
    pub(super) const fn reloaded(&self) -> &ParticipantConversationAggregate {
        &self.reloaded
    }

    /// Consumes the failure into the reconstructed durable aggregate.
    #[must_use]
    pub(super) fn into_reloaded(self) -> ParticipantConversationAggregate {
        self.reloaded
    }
}

/// Sole live owner of one stream head and non-cloneable protocol aggregate.
#[derive(Debug)]
pub(super) struct ParticipantConversationAggregate {
    stream: ConversationEventStream,
    stream_head: u64,
    conversation: ParticipantConversation,
}

impl ParticipantConversationAggregate {
    /// Cold-loads one conversation and durably appends protocol genesis only
    /// when replay proves the stream is new.
    ///
    /// Existing streams reach the protocol's one-shot refusal and append
    /// nothing. A new stream appends the exact canonical event before consuming
    /// its [`ConversationCommit`](liminal_protocol::lifecycle::ConversationCommit).
    /// If append fails, the commit is aborted and the stream is immediately
    /// replayed once from ordinal zero; no timer, backoff, or append retry exists.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationAggregateError`] for malformed or inconsistent
    /// durable history, a failed read, or a failed post-append cold reload.
    pub(super) async fn open(
        store: Arc<dyn DurableStore>,
        conversation_id: u64,
    ) -> Result<ConversationAggregateOpen, ConversationAggregateError> {
        let stream = ConversationEventStream::new(store, conversation_id);
        let conversation =
            ParticipantConversation::from_genesis(ConversationGenesis::new(conversation_id));
        let aggregate = replay_from_start(stream, conversation).await?;
        aggregate.initialize_genesis().await
    }

    /// Returns the stable owning conversation id.
    #[must_use]
    pub(super) const fn conversation_id(&self) -> u64 {
        self.conversation.conversation_id()
    }

    /// Returns the exact optimistic durable stream head.
    #[must_use]
    pub(super) const fn stream_head(&self) -> u64 {
        self.stream_head
    }

    /// Returns the protocol-owned next event ordinal.
    #[must_use]
    pub(super) const fn next_event_ordinal(&self) -> u64 {
        self.conversation.next_event_ordinal()
    }

    /// Reports whether protocol genesis validation is durable.
    #[must_use]
    pub(super) const fn genesis_validated(&self) -> bool {
        self.conversation.genesis_validated()
    }

    async fn initialize_genesis(
        self,
    ) -> Result<ConversationAggregateOpen, ConversationAggregateError> {
        let Self {
            stream,
            stream_head,
            conversation,
        } = self;
        match conversation.decide_genesis_validation() {
            ConversationDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let conversation = refusal.into_conversation();
                if reason == ConversationRefusalReason::GenesisAlreadyValidated {
                    Ok(ConversationAggregateOpen::Ready(Self {
                        stream,
                        stream_head,
                        conversation,
                    }))
                } else {
                    Err(ConversationAggregateError::GenesisRefused { reason })
                }
            }
            ConversationDecision::Commit(commit) => {
                let event_ordinal = commit.event().ordinal();
                if event_ordinal != stream_head {
                    let _unchanged = commit.abort();
                    return Err(ConversationAggregateError::PreparedEventOrdinal {
                        stream_head,
                        event_ordinal,
                    });
                }
                let payload = commit.event().encode_canonical();
                match stream.append(stream_head, payload).await {
                    Ok(next_stream_head) => {
                        let conversation = commit.commit();
                        Ok(ConversationAggregateOpen::Ready(Self {
                            stream,
                            stream_head: next_stream_head,
                            conversation,
                        }))
                    }
                    Err(append) => {
                        let unchanged = commit.abort();
                        let conversation_id = unchanged.conversation_id();
                        let cold = ParticipantConversation::from_genesis(ConversationGenesis::new(
                            conversation_id,
                        ));
                        match replay_from_start(stream, cold).await {
                            Ok(reloaded) => Ok(ConversationAggregateOpen::AppendFailed(
                                ConversationAggregateAppendFailure {
                                    error: append,
                                    reloaded,
                                },
                            )),
                            Err(reload) => {
                                Err(ConversationAggregateError::ReloadAfterAppendFailure {
                                    append,
                                    reload: Box::new(reload),
                                })
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn replay_from_start(
    stream: ConversationEventStream,
    mut conversation: ParticipantConversation,
) -> Result<ParticipantConversationAggregate, ConversationAggregateError> {
    let mut stream_head = 0_u64;
    loop {
        let page = stream.read_page(stream_head).await?;
        if page.is_empty() {
            return Ok(ParticipantConversationAggregate {
                stream,
                stream_head,
                conversation,
            });
        }
        let next_stream_head = page.next_sequence();
        for entry in page.into_entries() {
            let event = ConversationEvent::decode_canonical(&entry.payload).map_err(|reason| {
                ConversationAggregateError::EventDecode {
                    stored_sequence: entry.sequence,
                    reason,
                }
            })?;
            if event.ordinal() != entry.sequence {
                return Err(ConversationAggregateError::StoredEventOrdinal {
                    stored_sequence: entry.sequence,
                    event_ordinal: event.ordinal(),
                });
            }
            conversation = match conversation.replay(event) {
                Ok(resulting) => resulting,
                Err(failure) => {
                    return Err(ConversationAggregateError::Replay {
                        stored_sequence: entry.sequence,
                        reason: failure.reason(),
                    });
                }
            };
        }
        stream_head = next_stream_head;
    }
}
