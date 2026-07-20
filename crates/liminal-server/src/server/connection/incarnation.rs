//! Connection-supervisor ownership of the durable incarnation stream.
//!
//! The stream remains the storage binding and `liminal-protocol` remains the
//! allocator. This wrapper only bridges the server's synchronous startup and
//! accept seams to those async durable operations, serializes allocations, and
//! maps terminal protocol decisions into truthful server admission failures.

use std::sync::{Arc, Mutex};

use liminal::durability::{DurableStore, bridge::block_on};
use liminal_protocol::{outcome::ConnectionIncarnationExhausted, wire::ConnectionIncarnation};

use crate::ServerError;
use crate::server::participant::incarnation_stream::{
    IncarnationAllocation, IncarnationStartup, IncarnationStream, StartedIncarnationStream,
};

/// Started, fsynced, and serialized server-wide connection-incarnation source.
#[derive(Debug)]
pub(super) struct ConnectionIncarnationAuthority {
    state: Mutex<ConnectionIncarnationAuthorityState>,
}

#[derive(Debug)]
enum ConnectionIncarnationAuthorityState {
    Ready(StartedIncarnationStream),
    ConnectionOrdinalExhausted { attempted_server_incarnation: u64 },
    Failed,
}

impl ConnectionIncarnationAuthority {
    /// Wraps an already replayed stream for deterministic admission tests.
    #[cfg(test)]
    pub(super) const fn from_started_for_test(stream: StartedIncarnationStream) -> Self {
        Self {
            state: Mutex::new(ConnectionIncarnationAuthorityState::Ready(stream)),
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
    ) -> Result<Self, ServerError> {
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
            IncarnationStartup::Started(stream) => Ok(Self {
                state: Mutex::new(ConnectionIncarnationAuthorityState::Ready(stream)),
            }),
            IncarnationStartup::RecoveryRequired(recovery) => {
                let intents = recovery.intents();
                let Some(first) = intents.first() else {
                    return Err(ServerError::ParticipantIncarnation {
                        phase: "connection-fate recovery",
                        message: "recovery owner returned no unmatched Open".to_owned(),
                    });
                };
                Err(ServerError::ConnectionFateRecoveryRequired {
                    open_count: intents.len(),
                    first_open_sequence: first.open_sequence,
                })
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
        let mut state = self
            .state
            .lock()
            .map_err(|error| ServerError::ParticipantIncarnation {
                phase: "connection allocation lock",
                message: error.to_string(),
            })?;
        let current = core::mem::replace(&mut *state, ConnectionIncarnationAuthorityState::Failed);
        let result = match current {
            ConnectionIncarnationAuthorityState::Ready(mut stream) => {
                match block_on(stream.allocate(referenced_incarnations)) {
                    Err(error) => Err(ServerError::ParticipantIncarnation {
                        phase: "connection allocation bridge",
                        message: error.to_string(),
                    }),
                    Ok(Err(error)) => Err(ServerError::ParticipantIncarnation {
                        phase: "connection allocation persistence",
                        message: error.to_string(),
                    }),
                    Ok(Ok(IncarnationAllocation::Allocated {
                        connection_incarnation,
                        skipped_collisions: _,
                    })) => {
                        *state = ConnectionIncarnationAuthorityState::Ready(stream);
                        Ok(connection_incarnation)
                    }
                    Ok(Ok(IncarnationAllocation::Exhausted(
                        ConnectionIncarnationExhausted::ConnectionOrdinal {
                            attempted_server_incarnation,
                        },
                    ))) => {
                        *state =
                            ConnectionIncarnationAuthorityState::ConnectionOrdinalExhausted {
                                attempted_server_incarnation,
                            };
                        Err(ServerError::ConnectionIncarnationExhausted {
                            attempted_server_incarnation,
                        })
                    }
                    Ok(Ok(IncarnationAllocation::Exhausted(
                        ConnectionIncarnationExhausted::ServerIncarnation,
                    ))) => Err(ServerError::ServerIncarnationExhausted),
                }
            }
            ConnectionIncarnationAuthorityState::ConnectionOrdinalExhausted {
                attempted_server_incarnation,
            } => {
                *state = ConnectionIncarnationAuthorityState::ConnectionOrdinalExhausted {
                    attempted_server_incarnation,
                };
                Err(ServerError::ConnectionIncarnationExhausted {
                    attempted_server_incarnation,
                })
            }
            ConnectionIncarnationAuthorityState::Failed => {
                Err(ServerError::ParticipantIncarnation {
                    phase: "connection allocation unavailable",
                    message: "a prior allocation had an ambiguous durable result; process recovery is required"
                        .to_owned(),
                })
            }
        };
        drop(state);
        result
    }
}
