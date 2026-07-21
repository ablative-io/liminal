use liminal_protocol::lifecycle::{CommittedDiedTerminal, LiveFrontierOwner};
use liminal_protocol::wire::ParticipantId;

use super::binding_fate_completion::{StoredSpecificFateIntentExt, intent_matches_token};
use super::log::{StoredOrdinaryTerminalSource, StoredSpecificFateIntent};
use super::state::{
    ConversationAuthority, PendingBindingFate, PendingSpecificFate, PreparedOrdinaryFinalizer,
    StateError,
};

#[derive(Clone, Copy)]
enum FinalizerMeasurement {
    BeforeEnclosing,
    AfterFencedProof,
}

impl ConversationAuthority {
    /// Measures Ordinary before an enclosing finalizer removes its participant frontier.
    pub(super) fn prepare_pending_died_finalizer(
        &mut self,
        participant_id: ParticipantId,
        died_source_sequence: u64,
        terminal: CommittedDiedTerminal,
        source: StoredOrdinaryTerminalSource,
        owner: LiveFrontierOwner,
    ) -> Result<(LiveFrontierOwner, bool), StateError> {
        self.prepare_pending_died_finalizer_at(
            participant_id,
            died_source_sequence,
            terminal,
            source,
            FinalizerMeasurement::BeforeEnclosing,
            owner,
        )
    }

    /// Measures Ordinary after fenced proof minting consumes marker authority.
    pub(super) fn prepare_pending_died_fenced_finalizer(
        &mut self,
        participant_id: ParticipantId,
        died_source_sequence: u64,
        terminal: CommittedDiedTerminal,
        source: StoredOrdinaryTerminalSource,
        owner: LiveFrontierOwner,
    ) -> Result<(LiveFrontierOwner, bool), StateError> {
        self.prepare_pending_died_finalizer_at(
            participant_id,
            died_source_sequence,
            terminal,
            source,
            FinalizerMeasurement::AfterFencedProof,
            owner,
        )
    }

    fn prepare_pending_died_finalizer_at(
        &mut self,
        participant_id: ParticipantId,
        died_source_sequence: u64,
        terminal: CommittedDiedTerminal,
        source: StoredOrdinaryTerminalSource,
        measurement: FinalizerMeasurement,
        owner: LiveFrontierOwner,
    ) -> Result<(LiveFrontierOwner, bool), StateError> {
        let Some(pending) = self.pending_specific_fates.remove(&participant_id) else {
            return Ok((owner, false));
        };
        if pending.died_source_sequence != died_source_sequence
            || !matches!(pending.intent, StoredSpecificFateIntent::Ordinary { .. })
            || pending.terminal.is_some()
        {
            return Err(StateError::invariant(
                "pending Died finalizer disagrees with its open Ordinary intent",
            ));
        }
        let attached_source_sequence = pending.binding_fate.attached_source_sequence;
        if attached_source_sequence != pending.intent.attached_source_sequence()
            || !intent_matches_token(pending.intent, &pending.binding_fate)
        {
            return Err(StateError::invariant(
                "pending Died finalizer lost its sealed Ordinary authority",
            ));
        }
        let prepared = match measurement {
            FinalizerMeasurement::BeforeEnclosing => owner.prepare_pending_died_ordinary_finalizer(
                pending.binding_fate.token,
                terminal,
                self.observer_progress,
            ),
            FinalizerMeasurement::AfterFencedProof => owner
                .prepare_pending_died_ordinary_after_fenced_proof(
                    pending.binding_fate.token,
                    terminal,
                    self.observer_progress,
                ),
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(refused) => {
                let error = refused.error();
                let (owner, token, _) = refused.into_parts();
                self.pending_specific_fates.insert(
                    participant_id,
                    PendingSpecificFate {
                        died_source_sequence,
                        intent: pending.intent,
                        terminal: None,
                        binding_fate: PendingBindingFate {
                            attached_source_sequence,
                            token,
                        },
                    },
                );
                self.install_frontier(owner);
                return Err(StateError::invariant(format!(
                    "pending Died finalizer measurement refused: {error:?}"
                )));
            }
        };
        let (owner, fate, finalizer) = prepared.into_parts();
        if self
            .prepared_ordinary_finalizers
            .insert(
                participant_id,
                PreparedOrdinaryFinalizer {
                    attached_source_sequence,
                    terminal,
                    terminal_source: source,
                    fate,
                    finalizer,
                },
            )
            .is_some()
        {
            return Err(StateError::invariant(
                "pending Died finalizer replaced prepared Ordinary authority",
            ));
        }
        Ok((owner, true))
    }
}
