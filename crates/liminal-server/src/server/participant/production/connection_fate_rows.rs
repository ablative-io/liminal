//! Exact Died/Detached row construction from admitted terminal authority.

use liminal_protocol::lifecycle::{
    ActiveBinding, BindingState, BindingTerminalDisposition, CommittedDiedTerminal,
    DetachedBindingTransition, DiedBindingTransition, ObserverProgressProjection,
};

use crate::server::participant::ConnectionFateClass;

use super::connection_fate::ConnectionFateSource;
use super::log::{
    StoredDetached, StoredDetachedCause, StoredDetachedSource, StoredDied, StoredDiedCause,
    StoredOperation, StoredSpecificFateIntent, StoredTerminalDisposition,
};

pub(super) struct CompletedSourceOperation {
    pub(super) operation: StoredOperation,
    pub(super) binding_state: BindingState,
    pub(super) clear_fate_token: bool,
    pub(super) observer_projection: Option<ObserverProgressProjection>,
    pub(super) committed_died_terminal: Option<CommittedDiedTerminal>,
}

#[derive(Clone, Copy)]
struct DiedSourceRow {
    active: ActiveBinding,
    cause: StoredDiedCause,
    terminal_order: u64,
    disposition: StoredTerminalDisposition,
    connection_intent_sequence: Option<u64>,
    specific_fate_intent: Option<StoredSpecificFateIntent>,
}

pub(super) fn source_operation(
    source: ConnectionFateSource,
    active: ActiveBinding,
    disposition: BindingTerminalDisposition,
    stored_disposition: StoredTerminalDisposition,
    specific_fate_intent: Option<StoredSpecificFateIntent>,
) -> CompletedSourceOperation {
    match source {
        ConnectionFateSource::Open {
            open_sequence,
            class: ConnectionFateClass::CleanDisconnect,
            ..
        } => {
            let transition = active.clean_disconnect(disposition);
            detached_source_operation(
                active,
                StoredDetachedCause::CleanDeregister,
                open_sequence,
                disposition_order(disposition),
                stored_disposition,
                transition,
            )
        }
        ConnectionFateSource::Open {
            open_sequence,
            class: ConnectionFateClass::ServerShutdown,
            ..
        } => {
            let transition = active.server_shutdown(disposition);
            detached_source_operation(
                active,
                StoredDetachedCause::ServerShutdown,
                open_sequence,
                disposition_order(disposition),
                stored_disposition,
                transition,
            )
        }
        ConnectionFateSource::Open {
            open_sequence,
            class: ConnectionFateClass::ConnectionLost,
            ..
        } => {
            let transition = active.connection_lost(disposition);
            died_source_operation(
                DiedSourceRow {
                    active,
                    cause: StoredDiedCause::ConnectionLost,
                    terminal_order: disposition_order(disposition),
                    disposition: stored_disposition,
                    connection_intent_sequence: Some(open_sequence),
                    specific_fate_intent,
                },
                transition,
            )
        }
        ConnectionFateSource::Open {
            open_sequence,
            class: ConnectionFateClass::ProtocolError,
            ..
        } => {
            let transition = active.protocol_error(disposition);
            died_source_operation(
                DiedSourceRow {
                    active,
                    cause: StoredDiedCause::ProtocolError,
                    terminal_order: disposition_order(disposition),
                    disposition: stored_disposition,
                    connection_intent_sequence: Some(open_sequence),
                    specific_fate_intent,
                },
                transition,
            )
        }
        ConnectionFateSource::UncleanServerRestart { .. } => {
            let transition = active.unclean_server_restart(disposition);
            died_source_operation(
                DiedSourceRow {
                    active,
                    cause: StoredDiedCause::UncleanServerRestart {
                        prior_server_incarnation: active
                            .binding_epoch
                            .connection_incarnation
                            .server_incarnation,
                    },
                    terminal_order: disposition_order(disposition),
                    disposition: stored_disposition,
                    connection_intent_sequence: None,
                    specific_fate_intent,
                },
                transition,
            )
        }
    }
}

fn detached_source_operation(
    active: ActiveBinding,
    cause: StoredDetachedCause,
    connection_intent_sequence: u64,
    terminal_order: u64,
    disposition: StoredTerminalDisposition,
    transition: DetachedBindingTransition,
) -> CompletedSourceOperation {
    let observer_projection = transition.observer_progress_projection();
    CompletedSourceOperation {
        operation: StoredOperation::Detached {
            row: StoredDetached {
                participant_id: active.participant_id,
                binding_epoch: active.binding_epoch.into(),
                cause,
                terminal_order,
                disposition,
                source: StoredDetachedSource::ConnectionClose {
                    connection_intent_sequence,
                },
            },
        },
        binding_state: transition.binding_state(),
        clear_fate_token: true,
        observer_projection,
        committed_died_terminal: None,
    }
}

fn died_source_operation(
    input: DiedSourceRow,
    transition: DiedBindingTransition,
) -> CompletedSourceOperation {
    let observer_projection = transition.observer_progress_projection();
    let committed_died_terminal = match transition {
        DiedBindingTransition::Committed(terminal) => Some(terminal),
        DiedBindingTransition::Pending(_) => None,
    };
    CompletedSourceOperation {
        operation: StoredOperation::Died {
            row: StoredDied {
                participant_id: input.active.participant_id,
                binding_epoch: input.active.binding_epoch.into(),
                cause: input.cause,
                terminal_order: input.terminal_order,
                disposition: input.disposition,
                connection_intent_sequence: input.connection_intent_sequence,
                specific_fate_intent: input.specific_fate_intent,
                drained: None,
            },
        },
        binding_state: transition.binding_state(),
        clear_fate_token: false,
        observer_projection,
        committed_died_terminal,
    }
}

const fn disposition_order(disposition: BindingTerminalDisposition) -> u64 {
    match disposition {
        BindingTerminalDisposition::Committed(position) => position.transaction_order(),
        BindingTerminalDisposition::Pending(position) => position.transaction_order(),
    }
}
