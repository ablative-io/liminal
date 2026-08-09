//! Connection-supervisor ownership of the durable incarnation stream.
//!
//! The stream remains the storage binding and `liminal-protocol` remains the
//! allocator. This wrapper only bridges the server's synchronous startup and
//! accept seams to those async durable operations, serializes allocations, and
//! maps terminal protocol decisions into truthful server admission failures.

use std::sync::{Arc, Mutex, MutexGuard};

use liminal::durability::{DurableStore, bridge::block_on};
use liminal_protocol::{outcome::ConnectionIncarnationExhausted, wire::ConnectionIncarnation};

use crate::ServerError;
use crate::server::participant::ParticipantSemanticHandler;
use crate::server::participant::incarnation_stream::{
    ConnectionFateClass, ConnectionFateIntent, DurableWriteReach, IncarnationAllocation,
    IncarnationOperationError, IncarnationStartup, IncarnationStream, StartedIncarnationStream,
};

/// `ServerError::ParticipantIncarnation::phase` for a refusal caused by an
/// unresolved ambiguous durable write.
///
/// Shared with the admission-refusal classifier
/// ([`crate::server::connection::refusal`]) so the wire reason, the metric label
/// and the message an operator reads are joined mechanically and cannot drift
/// apart the way a duplicated string literal would.
pub(super) const AMBIGUOUS_DURABLE_WRITE_PHASE: &str = "connection allocation unavailable";

/// Started, fsynced, and serialized server-wide connection-incarnation source.
#[derive(Debug)]
pub(super) struct ConnectionIncarnationAuthority {
    state: Mutex<ConnectionIncarnationAuthorityState>,
    maximum_conversations: usize,
}

#[derive(Debug)]
enum ConnectionIncarnationAuthorityState {
    Ready(StartedIncarnationStream),
    ConnectionOrdinalExhausted {
        attempted_server_incarnation: u64,
    },
    /// An append or flush had an unknown outcome, so this process no longer
    /// knows the stream's true head and must not append through this handle.
    AmbiguousDurableWrite {
        /// Display of the failure that armed the hold, kept so every refusal
        /// names its cause instead of a generic sentence.
        armed_by: String,
    },
}

impl ConnectionIncarnationAuthority {
    /// Wraps an already replayed stream for deterministic admission tests.
    #[cfg(test)]
    pub(super) const fn from_started_for_test(
        stream: StartedIncarnationStream,
        maximum_conversations: usize,
    ) -> Self {
        Self {
            state: Mutex::new(ConnectionIncarnationAuthorityState::Ready(stream)),
            maximum_conversations,
        }
    }

    /// Replays and fsyncs the server-incarnation transition before returning.
    ///
    /// # Errors
    ///
    /// Returns a typed startup exhaustion or durable-incarnation error. No
    /// listener may become ready when this construction fails.
    pub(super) fn startup(
        store: Arc<dyn DurableStore>,
        maximum_references: usize,
        maximum_conversations: u64,
        handler: &dyn ParticipantSemanticHandler,
    ) -> Result<Self, ServerError> {
        let maximum_conversations = usize::try_from(maximum_conversations).map_err(|error| {
            ServerError::ParticipantIncarnation {
                phase: "connection-fate conversation bound",
                message: error.to_string(),
            }
        })?;
        let startup = block_on(IncarnationStream::new(store, maximum_references).startup())
            .map_err(|error| ServerError::ParticipantIncarnation {
                phase: "server startup bridge",
                message: error.to_string(),
            })?
            .map_err(|error| ServerError::ParticipantIncarnation {
                phase: "server startup persistence",
                message: error.to_string(),
            })?;
        match startup {
            IncarnationStartup::Started(stream) => {
                Self::finish_startup(stream, maximum_conversations, handler)
            }
            IncarnationStartup::RecoveryRequired(mut recovery) => {
                let intents = recovery.intents();
                if intents.is_empty() {
                    return Err(ServerError::ParticipantIncarnation {
                        phase: "connection-fate recovery",
                        message: "recovery owner returned no unmatched Open".to_owned(),
                    });
                }
                for intent in intents {
                    handler
                        .handle_connection_fate(intent.work_item())
                        .map_err(|error| ServerError::ParticipantIncarnation {
                            phase: "connection-fate handler recovery",
                            message: format!(
                                "Open {} failed before Complete: {error}",
                                intent.open_sequence
                            ),
                        })?;
                    block_on(recovery.complete(intent.open_sequence))
                        .map_err(|error| ServerError::ParticipantIncarnation {
                            phase: "connection-fate Complete bridge",
                            message: error.to_string(),
                        })?
                        .map_err(|error| ServerError::ParticipantIncarnation {
                            phase: "connection-fate Complete persistence",
                            message: error.to_string(),
                        })?;
                }
                let resumed = block_on(recovery.finish_startup())
                    .map_err(|error| ServerError::ParticipantIncarnation {
                        phase: "post-recovery startup bridge",
                        message: error.to_string(),
                    })?
                    .map_err(|error| ServerError::ParticipantIncarnation {
                        phase: "post-recovery startup persistence",
                        message: error.to_string(),
                    })?;
                match resumed {
                    IncarnationStartup::Started(stream) => {
                        Self::finish_startup(stream, maximum_conversations, handler)
                    }
                    IncarnationStartup::RecoveryRequired(_) => {
                        Err(ServerError::ParticipantIncarnation {
                            phase: "post-recovery startup",
                            message: "completed recovery returned another unmatched Open set"
                                .to_owned(),
                        })
                    }
                    IncarnationStartup::Exhausted(
                        ConnectionIncarnationExhausted::ServerIncarnation,
                    ) => Err(ServerError::ServerIncarnationExhausted),
                    IncarnationStartup::Exhausted(
                        ConnectionIncarnationExhausted::ConnectionOrdinal {
                            attempted_server_incarnation,
                        },
                    ) => Err(ServerError::ParticipantIncarnation {
                        phase: "post-recovery startup protocol",
                        message: format!(
                            "unexpected connection-ordinal exhaustion for server incarnation {attempted_server_incarnation}"
                        ),
                    }),
                }
            }
            IncarnationStartup::Exhausted(ConnectionIncarnationExhausted::ServerIncarnation) => {
                Err(ServerError::ServerIncarnationExhausted)
            }
            IncarnationStartup::Exhausted(ConnectionIncarnationExhausted::ConnectionOrdinal {
                attempted_server_incarnation,
            }) => Err(ServerError::ParticipantIncarnation {
                phase: "server startup protocol",
                message: format!(
                    "unexpected connection-ordinal exhaustion for server incarnation \
                     {attempted_server_incarnation} during startup"
                ),
            }),
        }
    }

    fn finish_startup(
        stream: StartedIncarnationStream,
        maximum_conversations: usize,
        handler: &dyn ParticipantSemanticHandler,
    ) -> Result<Self, ServerError> {
        handler
            .repair_unclean_server_restart(stream.server_incarnation())
            .map_err(|error| ServerError::ParticipantIncarnation {
                phase: "unclean-server-restart repair",
                message: error.to_string(),
            })?;
        Ok(Self {
            state: Mutex::new(ConnectionIncarnationAuthorityState::Ready(stream)),
            maximum_conversations,
        })
    }

    /// Allocates and fsyncs one collision-free connection incarnation.
    ///
    /// `referenced_incarnations` must be the complete bounded set available to
    /// the caller. The mutex spans the protocol decision and durable append, so
    /// concurrent accepts cannot publish the same pair.
    ///
    /// # Errors
    ///
    /// Returns a typed connection-ordinal exhaustion or durable-incarnation
    /// failure. The accepted socket must be dropped rather than admitted.
    pub(super) fn allocate(
        &self,
        referenced_incarnations: &[ConnectionIncarnation],
    ) -> Result<ConnectionIncarnation, ServerError> {
        let mut state = Self::lock(&self.state, "connection allocation lock")?;
        let ConnectionIncarnationAuthorityState::Ready(stream) = &mut *state else {
            return Err(Self::not_ready_refusal(&state));
        };
        let outcome = block_on(stream.allocate(referenced_incarnations));
        let result = match outcome {
            // A bridge failure means the future did not run to completion, so
            // whether its append was issued is exactly what is not known. R1(a)
            // rules an indeterminate class ambiguous: latching is the safe side.
            Err(error) => Err(Self::arm_hold(
                &mut state,
                "connection allocation bridge",
                &error.to_string(),
            )),
            Ok(Err(failure)) => Err(Self::classify(
                &mut state,
                "connection allocation persistence",
                failure,
            )),
            Ok(Ok(IncarnationAllocation::Allocated {
                connection_incarnation,
                skipped_collisions: _,
            })) => Ok(connection_incarnation),
            Ok(Ok(IncarnationAllocation::Exhausted(
                ConnectionIncarnationExhausted::ConnectionOrdinal {
                    attempted_server_incarnation,
                },
            ))) => {
                *state = ConnectionIncarnationAuthorityState::ConnectionOrdinalExhausted {
                    attempted_server_incarnation,
                };
                Err(ServerError::ConnectionIncarnationExhausted {
                    attempted_server_incarnation,
                })
            }
            Ok(Ok(IncarnationAllocation::Exhausted(
                ConnectionIncarnationExhausted::ServerIncarnation,
            ))) => Err(ServerError::ServerIncarnationExhausted),
        };
        drop(state);
        result
    }

    /// Opens and flushes one bounded connection-fate intent before teardown work.
    ///
    /// The declared bound is always the signed participant configuration captured
    /// at authority construction; callers can supply only the observed sorted set.
    ///
    /// # Errors
    ///
    /// Returns a typed lock, validation, append, or flush failure. Any ambiguous
    /// durable result permanently fails this process-local authority.
    pub(super) fn open_connection_fate(
        &self,
        connection_incarnation: ConnectionIncarnation,
        class: ConnectionFateClass,
        conversations: &[u64],
    ) -> Result<ConnectionFateIntent, ServerError> {
        let mut state = Self::lock(&self.state, "connection-fate Open lock")?;
        let ConnectionIncarnationAuthorityState::Ready(stream) = &mut *state else {
            return Err(Self::not_ready_refusal(&state));
        };
        let outcome = block_on(stream.open_connection_fate(
            connection_incarnation,
            class,
            self.maximum_conversations,
            conversations,
        ));
        let result = match outcome {
            Err(error) => Err(Self::arm_hold(
                &mut state,
                "connection-fate Open bridge",
                &error.to_string(),
            )),
            Ok(Err(failure)) => Err(Self::classify(
                &mut state,
                "connection-fate Open persistence",
                failure,
            )),
            Ok(Ok(intent)) => Ok(intent),
        };
        drop(state);
        result
    }

    /// Appends and flushes Complete after every Open target has durably finished.
    ///
    /// # Errors
    ///
    /// Returns a typed lock, absent-Open, append, or flush failure. Any ambiguous
    /// durable result permanently fails this process-local authority.
    pub(super) fn complete_connection_fate(&self, open_sequence: u64) -> Result<(), ServerError> {
        let mut state = Self::lock(&self.state, "connection-fate Complete lock")?;
        let ConnectionIncarnationAuthorityState::Ready(stream) = &mut *state else {
            return Err(Self::not_ready_refusal(&state));
        };
        let outcome = block_on(stream.complete_connection_fate(open_sequence));
        let result = match outcome {
            Err(error) => Err(Self::arm_hold(
                &mut state,
                "connection-fate Complete bridge",
                &error.to_string(),
            )),
            Ok(Err(failure)) => Err(Self::classify(
                &mut state,
                "connection-fate Complete persistence",
                failure,
            )),
            Ok(Ok(())) => Ok(()),
        };
        drop(state);
        result
    }

    /// Locks the shared state, mapping poisoning to a typed phase failure.
    fn lock<'guard>(
        state: &'guard Mutex<ConnectionIncarnationAuthorityState>,
        phase: &'static str,
    ) -> Result<MutexGuard<'guard, ConnectionIncarnationAuthorityState>, ServerError> {
        state.lock().map_err(|error| {
            ServerError::ParticipantIncarnation {
                phase,
                message: error.to_string(),
            }
        })
    }

    /// Turns one failed operation into the right STATE transition.
    ///
    /// This is the whole of R1(a). The old code moved the authority to `Failed`
    /// before every operation and restored it only on success, so a validation
    /// refusal — a Complete for an absent Open, an over-bound reference set, an
    /// encoding overflow — disarmed admission for the process just as surely as
    /// a half-written fsync did. The stream now reports how far it got, and only
    /// [`DurableWriteReach::Ambiguous`] takes the authority out of service. A
    /// [`DurableWriteReach::NotAttempted`] failure leaves the handle exactly
    /// where it was, because nothing about it became untrue: ONE operation
    /// failed, and the next connection is unaffected.
    fn classify(
        state: &mut ConnectionIncarnationAuthorityState,
        phase: &'static str,
        failure: IncarnationOperationError,
    ) -> ServerError {
        match failure.reach {
            DurableWriteReach::NotAttempted => {
                tracing::warn!(
                    phase,
                    error = %failure.error,
                    "incarnation-stream operation refused before any durable write; admission unaffected"
                );
                ServerError::ParticipantIncarnation {
                    phase,
                    message: failure.error.to_string(),
                }
            }
            DurableWriteReach::Ambiguous => Self::arm_hold(state, phase, &failure.error.to_string()),
        }
    }

    /// Takes the authority out of service pending a resume replay.
    ///
    /// Loud on purpose: this is the moment the server stops admitting, and the
    /// field boot that refused 82,166 connections in a row logged nothing at all
    /// at the moment it decided to.
    fn arm_hold(
        state: &mut ConnectionIncarnationAuthorityState,
        phase: &'static str,
        message: &str,
    ) -> ServerError {
        if !matches!(state, ConnectionIncarnationAuthorityState::Ready(_)) {
            // Already held or exhausted; the existing refusal is the truthful one.
            return Self::not_ready_refusal(state);
        }
        tracing::error!(
            phase,
            error = message,
            "incarnation stream had an AMBIGUOUS durable result: admission is held"
        );
        *state = ConnectionIncarnationAuthorityState::AmbiguousDurableWrite {
            armed_by: message.to_owned(),
        };
        ServerError::ParticipantIncarnation {
            phase,
            message: message.to_owned(),
        }
    }

    /// The refusal an arriving connection receives while the hold is in force.
    fn held_refusal(armed_by: &str) -> ServerError {
        ServerError::ParticipantIncarnation {
            phase: AMBIGUOUS_DURABLE_WRITE_PHASE,
            message: format!(
                "a prior incarnation-stream operation had an ambiguous durable result \
                 ({armed_by}); process recovery is required"
            ),
        }
    }

    /// The refusal for a state that is not `Ready` and was not made ready.
    fn not_ready_refusal(state: &ConnectionIncarnationAuthorityState) -> ServerError {
        match state {
            ConnectionIncarnationAuthorityState::ConnectionOrdinalExhausted {
                attempted_server_incarnation,
            } => ServerError::ConnectionIncarnationExhausted {
                attempted_server_incarnation: *attempted_server_incarnation,
            },
            ConnectionIncarnationAuthorityState::AmbiguousDurableWrite { armed_by } => {
                Self::held_refusal(armed_by)
            }
            ConnectionIncarnationAuthorityState::Ready(_) => {
                ServerError::ParticipantIncarnation {
                    phase: "connection allocation state",
                    message: "authority reported not-ready while holding a ready stream".to_owned(),
                }
            }
        }
    }
}
