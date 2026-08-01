//! Lossless live connection-fate impact orchestration.

use liminal_protocol::lifecycle::BindingTerminalAdmitError;

use crate::server::participant::dispatch_impact::{DispatchImpact, DispatchImpactAccumulator};
use crate::server::participant::{
    ConnectionFateWorkItem, ParticipantConnectionFateOutcome, ParticipantSemanticError,
};

use super::handler::ProductionParticipantHandler;
use super::state::StateError;

impl ProductionParticipantHandler {
    pub(super) fn apply_connection_fate_with_impacts(
        &self,
        work_item: &ConnectionFateWorkItem,
    ) -> ParticipantConnectionFateOutcome {
        let mut impacts = Vec::new();
        if let Err(error) = self.ensure_service_live() {
            return ParticipantConnectionFateOutcome::new(Err(error), impacts);
        }
        for conversation_id in work_item.tracked_conversations.iter().copied() {
            let mut impact = DispatchImpactAccumulator::new();
            let result = self.with_conversation_fate_source(
                conversation_id,
                Some(&mut impact),
                |authority, appender, impact| {
                    let impact = impact.ok_or_else(|| {
                        StateError::invariant("live connection fate lost its impact owner")
                    })?;
                    let transaction = authority.prepare_connection_fate_transaction(work_item);
                    transaction.complete_with_impact(authority, appender, impact)
                },
            );
            if let Some(changed) = changed_impact(impact.finish(conversation_id)) {
                impacts.push(changed);
            }
            if let Err(error) = result {
                if matches!(
                    error,
                    ParticipantSemanticError::BindingTerminalAdmissionRefused {
                        error: BindingTerminalAdmitError::Precedence,
                    }
                ) {
                    // The conversation's immutable-candidate lane is occupied.
                    // The already-durable Open is the retry token, so the whole
                    // work item is replayed later — by boot recovery at
                    // connection/incarnation.rs:88, and once the lane drains at
                    // its sole production emptier, ops_frontier.rs:142 on the
                    // next successful publish. Latching the process-wide fatal
                    // here would refuse that publish too, so the lane could
                    // never drain and the replay could never converge. The
                    // conversations already committed above are a no-op on
                    // replay: their slots left Bound, so the re-run's prepared
                    // target set for this connection is empty and appends
                    // nothing. The typed refusal travels on unchanged.
                    return ParticipantConnectionFateOutcome::new(Err(error), impacts);
                }
                let fatal = match self
                    .latch_connection_fate_fatal(work_item.open_sequence, conversation_id)
                {
                    Ok(fatal) => fatal,
                    Err(latch_error) => {
                        return ParticipantConnectionFateOutcome::new(Err(latch_error), impacts);
                    }
                };
                let error = ParticipantSemanticError::Internal {
                    message: format!("{fatal}: {error}"),
                };
                return ParticipantConnectionFateOutcome::new(Err(error), impacts);
            }
        }
        ParticipantConnectionFateOutcome::new(Ok(()), impacts)
    }
}

fn changed_impact(impact: DispatchImpact) -> Option<DispatchImpact> {
    match impact {
        DispatchImpact::Unchanged => None,
        changed @ DispatchImpact::Changed { .. } => Some(changed),
    }
}
