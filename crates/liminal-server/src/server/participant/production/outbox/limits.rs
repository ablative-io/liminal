//! Checked signed bounds and prospective live accounting for one outbox owner.

use liminal_protocol::wire::ParticipantId;

use super::{ConversationOutbox, ConversationOutboxError, ProjectedRecord};

/// Signed per-conversation bounds installed with every live or restored outbox owner.
#[derive(Clone, Copy, Debug)]
pub(in crate::server::participant::production) struct ConversationOutboxLimits {
    pub(super) max_live_recipient_obligations: u64,
}

impl ConversationOutboxLimits {
    pub(in crate::server::participant::production) fn try_new(
        max_retained_record_rows: u64,
        identity_slots: u64,
    ) -> Result<Self, ConversationOutboxError> {
        let max_live_recipient_obligations = max_retained_record_rows
            .checked_mul(identity_slots)
            .ok_or(ConversationOutboxError::BoundOverflow {
            name: "UNIT2_MAX_LIVE_RECIPIENT_OBLIGATIONS",
        })?;
        Ok(Self {
            max_live_recipient_obligations,
        })
    }
}

pub(super) fn ensure_live_obligation_capacity(
    owner: &ConversationOutbox,
    records: &[ProjectedRecord],
    retiring: Option<ParticipantId>,
) -> Result<(), ConversationOutboxError> {
    let discharged = retiring.map_or(0, |participant_id| {
        owner
            .records
            .values()
            .filter(|record| record.recipients.contains(&participant_id))
            .count()
    });
    let discharged =
        u64::try_from(discharged).map_err(|_| ConversationOutboxError::ChargeOverflow)?;
    let added = records.iter().try_fold(0_u64, |total, record| {
        let count = u64::try_from(record.recipients().len())
            .map_err(|_| ConversationOutboxError::ChargeOverflow)?;
        total
            .checked_add(count)
            .ok_or(ConversationOutboxError::ChargeOverflow)
    })?;
    let attempted = owner
        .live_recipient_obligations
        .checked_sub(discharged)
        .and_then(|current| current.checked_add(added))
        .ok_or(ConversationOutboxError::ChargeOverflow)?;
    if attempted > owner.limits.max_live_recipient_obligations {
        return Err(ConversationOutboxError::LiveRecipientObligationsExceeded {
            limit: owner.limits.max_live_recipient_obligations,
            attempted,
        });
    }
    Ok(())
}
