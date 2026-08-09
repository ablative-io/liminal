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
    IncarnationOperationError, IncarnationStartup, IncarnationStream, IncarnationStreamError,
    StartedIncarnationStream,
};

/// `ServerError::ParticipantIncarnation::phase` for a refusal caused by an
/// unresolved ambiguous durable write.
///
/// Shared with the admission-refusal classifier
/// ([`crate::server::connection::refusal`]) so the wire reason, the metric label
/// and the message an operator reads are joined mechanically and cannot drift
/// apart the way a duplicated string literal would.
pub(super) const AMBIGUOUS_DURABLE_WRITE_PHASE: &str = "connection allocation unavailable";

/// `ServerError::ParticipantIncarnation::phase` for a permanently surrendered
/// authority — the stream moved to a server incarnation this process does not
/// own, so no amount of re-reading will make it usable again.
pub(super) const AUTHORITY_SURRENDERED_PHASE: &str = "connection allocation surrendered";

/// Admission attempts refused before the FIRST resume replay is attempted.
///
/// Zero, deliberately: the connection that arrives immediately after an
/// ambiguous write pays for the re-read, because the alternative is refusing a
/// connection the server could have served while the store was already healthy.
const RESUME_BACKOFF_INITIAL: u32 = 0;

/// Ceiling on the number of admission attempts refused between resume replays.
///
/// A resume replay reads the whole incarnation stream while holding the
/// authority mutex, so a store that is hard down must not be re-read once per
/// arriving connection. The window doubles on each failed resume up to this
/// bound and resets on success, so a transient store costs one replay and a
/// dead one costs a replay roughly every thousand attempts — at the field rate
/// of about one connection per second, roughly every seventeen minutes. It never
/// stops trying: an unowned "process recovery is required" is an indefinite
/// hold, which is the defect this bounds rather than a property to preserve.
const RESUME_BACKOFF_CEILING: u32 = 1024;

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
    /// An append or flush had an unknown outcome. Admission is refused until a
    /// replay re-establishes ground truth — this is a HOLD, not a grave.
    AmbiguousDurableWrite(AmbiguousDurableWrite),
    /// The durable stream is owned by another server incarnation. Nothing this
    /// process can read will change that, so this state is terminal on purpose.
    Surrendered {
        message: String,
    },
}

/// Everything needed to go back to the store and find out what really happened.
#[derive(Debug)]
struct AmbiguousDurableWrite {
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
    /// Server incarnation this process durably started under. A resume that
    /// replays to a different one is refused rather than adopted.
    server_incarnation: u64,
    /// Display of the failure that armed the hold, kept so every refusal while
    /// held names its cause instead of a generic sentence.
    armed_by: String,
    /// Admission attempts still to refuse before the next resume replay.
    attempts_until_resume: u32,
    /// Current backoff window, doubled on each failed resume.
    resume_backoff: u32,
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
        Self::make_ready(&mut state)?;
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
        // Deliberately NO resume attempt here. This is the TEARDOWN path, and
        // teardown must not depend on the authority being healthy: a shutdown
        // path that needs the thing that is broken is a shutdown path that
        // cannot run when it is most needed. A held authority fast-fails here,
        // the caller's `fail_fate` crashes that one connection, and its record
        // removal wakes the drain — so a held authority makes teardown FASTER,
        // which is the behaviour that must survive this lane. Admission pays
        // for the replay (see `allocate`); a force-close of every live
        // connection does not.
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
        // No resume attempt: teardown path, same reasoning as `open_connection_fate`.
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
        let ConnectionIncarnationAuthorityState::Ready(stream) = state else {
            // Already held or exhausted; the existing refusal is the truthful one.
            return Self::not_ready_refusal(state);
        };
        tracing::error!(
            phase,
            error = message,
            "incarnation stream had an AMBIGUOUS durable result: admission is held until a replay \
             re-establishes ground truth"
        );
        *state =
            ConnectionIncarnationAuthorityState::AmbiguousDurableWrite(AmbiguousDurableWrite {
                store: stream.store(),
                maximum_references: stream.maximum_references(),
                server_incarnation: stream.server_incarnation(),
                armed_by: message.to_owned(),
                attempts_until_resume: RESUME_BACKOFF_INITIAL,
                resume_backoff: 1,
            });
        ServerError::ParticipantIncarnation {
            phase,
            message: message.to_owned(),
        }
    }

    /// Resolves the ambiguity by LOOKING, when the backoff window allows it.
    ///
    /// R1(b). "Process recovery is required" names a trigger nothing inside the
    /// process owns, which makes it an indefinite hold rather than a recovery
    /// procedure. The ambiguity is a question about the durable stream, and the
    /// durable stream can be read, so the resolution is a replay: whatever the
    /// half-finished append did or did not do, the store now says which.
    ///
    /// The replay runs under the authority mutex, so it serialises against every
    /// other admission — the same way an ordinary allocation's append and fsync
    /// already do — and the backoff bounds how often a hard-down store pays for
    /// it.
    fn make_ready(state: &mut ConnectionIncarnationAuthorityState) -> Result<(), ServerError> {
        let ConnectionIncarnationAuthorityState::AmbiguousDurableWrite(held) = state else {
            // Ready, exhausted, or surrendered: nothing to resolve here. The
            // callers' `else` arm turns the latter two into their own refusals.
            return Ok(());
        };
        if held.attempts_until_resume > 0 {
            held.attempts_until_resume -= 1;
            return Err(Self::held_refusal(held));
        }
        let stream = IncarnationStream::new(Arc::clone(&held.store), held.maximum_references);
        let resumed = block_on(stream.resume_after_ambiguous_write(held.server_incarnation));
        match resumed {
            Ok(Ok(started)) => {
                tracing::warn!(
                    server_incarnation = started.server_incarnation(),
                    armed_by = held.armed_by,
                    "incarnation stream RESUMED from durable ground truth; admission restored"
                );
                *state = ConnectionIncarnationAuthorityState::Ready(started);
                Ok(())
            }
            Ok(Err(IncarnationStreamError::ResumeServerIncarnationMoved { expected, actual })) => {
                let message = format!(
                    "durable incarnation stream moved from server incarnation {expected} to \
                     {actual}: another process owns it and this one must not allocate against it"
                );
                tracing::error!(message, "incarnation authority SURRENDERED");
                *state = ConnectionIncarnationAuthorityState::Surrendered {
                    message: message.clone(),
                };
                Err(ServerError::ParticipantIncarnation {
                    phase: AUTHORITY_SURRENDERED_PHASE,
                    message,
                })
            }
            Ok(Err(error)) => Err(Self::back_off(held, &error.to_string())),
            Err(error) => Err(Self::back_off(held, &error.to_string())),
        }
    }

    /// Widens the resume window after a failed replay and refuses this attempt.
    fn back_off(held: &mut AmbiguousDurableWrite, resume_error: &str) -> ServerError {
        held.resume_backoff = held.resume_backoff.saturating_mul(2).min(RESUME_BACKOFF_CEILING);
        held.attempts_until_resume = held.resume_backoff;
        tracing::warn!(
            resume_error,
            armed_by = held.armed_by,
            next_resume_after_attempts = held.attempts_until_resume,
            "incarnation-stream resume replay failed; admission stays held"
        );
        Self::held_refusal(held)
    }

    /// The refusal an arriving connection receives while the hold is in force.
    fn held_refusal(held: &AmbiguousDurableWrite) -> ServerError {
        ServerError::ParticipantIncarnation {
            phase: AMBIGUOUS_DURABLE_WRITE_PHASE,
            message: format!(
                "a prior incarnation-stream operation had an ambiguous durable result \
                 ({}); admission is held and a resume replay is retried on later attempts",
                held.armed_by
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
            ConnectionIncarnationAuthorityState::Surrendered { message } => {
                ServerError::ParticipantIncarnation {
                    phase: AUTHORITY_SURRENDERED_PHASE,
                    message: message.clone(),
                }
            }
            ConnectionIncarnationAuthorityState::AmbiguousDurableWrite(held) => {
                Self::held_refusal(held)
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
