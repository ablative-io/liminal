use liminal_protocol::lifecycle::{
    BindingState, CommittedDiedTerminal, LiveFrontierOwner, PendingFinalization,
};
use liminal_protocol::wire::{CloseCause, CredentialAttachRequest, ParticipantId};

use super::fate_occurrence::{FateOccurrenceKey, PendingFinalizerRoute};
use super::log::{
    StoredAttachModeV3, StoredComposedTerminalCause, StoredComposedTerminalKind,
    StoredFinalizerPresentation, StoredOrdinaryTerminalSource, StoredPendingDiedFinalizer,
};
use super::state::{ConversationAuthority, StateError};

#[derive(Clone, Copy)]
pub(super) struct SelectedFencedFinalizer {
    route: PendingFinalizerRoute,
    committed_died_terminal: Option<CommittedDiedTerminal>,
}

impl SelectedFencedFinalizer {
    pub(super) const fn is_non_presenting(self) -> bool {
        matches!(
            self.route.presentation,
            StoredFinalizerPresentation::ConsumeRecoveredReservation { .. }
        )
    }
}

impl ConversationAuthority {
    pub(super) fn prepare_selected_fenced_finalizer(
        &mut self,
        participant_id: ParticipantId,
        source_sequence: u64,
        finalizer: Option<SelectedFencedFinalizer>,
        owner: LiveFrontierOwner,
    ) -> Result<(LiveFrontierOwner, bool), StateError> {
        match finalizer {
            Some(SelectedFencedFinalizer {
                route,
                committed_died_terminal: Some(terminal),
            }) => self.prepare_pending_died_fenced_finalizer(
                participant_id,
                route.pending_source_sequence,
                terminal,
                StoredOrdinaryTerminalSource::PendingDiedFinalized {
                    died_source_sequence: route.pending_source_sequence,
                    finalizer: StoredPendingDiedFinalizer::FencedAttached { source_sequence },
                },
                owner,
            ),
            Some(_) | None => Ok((owner, false)),
        }
    }

    pub(super) fn select_fenced_finalizer(
        &mut self,
        binding: BindingState,
        mode: &StoredAttachModeV3,
        request: &CredentialAttachRequest,
    ) -> Result<Option<SelectedFencedFinalizer>, StateError> {
        let BindingState::PendingFinalization(pending) = binding else {
            return Ok(None);
        };
        let StoredAttachModeV3::Fenced {
            composed_terminal: Some(terminal),
            ..
        } = mode
        else {
            return Err(StateError::invariant(
                "pending attach finalizer requires a composed fenced terminal",
            ));
        };
        let route = self.fate_occurrences.select_finalizer(FateOccurrenceKey {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            binding_epoch: pending.binding_epoch(),
        })?;
        let expected_kind = match pending {
            PendingFinalization::Died(_) => StoredComposedTerminalKind::Died,
            PendingFinalization::Detached(_) => StoredComposedTerminalKind::Detached,
        };
        let expected_cause = stored_composed_cause(pending.close_cause())?;
        if terminal.kind != expected_kind
            || terminal.cause != expected_cause
            || terminal.transaction_order != pending.admission_order().transaction_order()
            || terminal.pending_source_sequence != route.pending_source_sequence
            || terminal.presentation != route.presentation
        {
            return Err(StateError::invariant(
                "fenced attach composed terminal differs from selected pending finalizer",
            ));
        }
        let committed_died_terminal = match pending {
            PendingFinalization::Died(died) => Some(died.commit(terminal.delivery_seq)),
            PendingFinalization::Detached(_) => None,
        };
        Ok(Some(SelectedFencedFinalizer {
            route,
            committed_died_terminal,
        }))
    }
}

fn stored_composed_cause(cause: CloseCause) -> Result<StoredComposedTerminalCause, StateError> {
    match cause {
        CloseCause::CleanDeregister => Ok(StoredComposedTerminalCause::CleanDeregister),
        CloseCause::ConnectionLost => Ok(StoredComposedTerminalCause::ConnectionLost),
        CloseCause::ProcessKilled => Ok(StoredComposedTerminalCause::ProcessKilled),
        CloseCause::ProtocolError => Ok(StoredComposedTerminalCause::ProtocolError),
        CloseCause::ServerShutdown => Ok(StoredComposedTerminalCause::ServerShutdown),
        CloseCause::UncleanServerRestart {
            prior_server_incarnation,
        } => Ok(StoredComposedTerminalCause::UncleanServerRestart {
            prior_server_incarnation,
        }),
        CloseCause::Superseded => Err(StateError::invariant(
            "superseded terminal cannot be a pending fenced finalizer",
        )),
    }
}
