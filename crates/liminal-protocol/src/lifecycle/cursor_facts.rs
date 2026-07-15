use alloc::{collections::BTreeMap, vec::Vec};

use crate::wire::{DeliverySeq, ParticipantIndex};

/// Participant-scoped cursor progress key mandated by extraction Fix 2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CursorProgressKey {
    /// Permanent participant index.
    pub participant_index: ParticipantIndex,
    /// Requested cumulative boundary.
    pub boundary: DeliverySeq,
}

/// Durable cursor-progress fact state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorProgressFact {
    /// Boundary remains independently fireable for this participant.
    Pending,
    /// Boundary was covered by this participant's committed cumulative ack.
    Consumed,
}

/// Variable participant-scoped cursor facts; no fixed occurrence array exists.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CursorProgressFacts {
    facts: BTreeMap<CursorProgressKey, CursorProgressFact>,
}

/// Deterministic cursor-fact serialization failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorFactEncodeError {
    /// Fact count cannot fit the variable format's `u32` count.
    TooManyFacts,
    /// Encoded byte length overflowed the platform allocation domain.
    LengthOverflow,
}

impl CursorProgressFacts {
    /// Creates an empty participant-scoped fact map.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            facts: BTreeMap::new(),
        }
    }

    /// Inserts an independently fireable `(participant_index, boundary)` fact.
    ///
    /// Returns `true` only when the exact pair was not already present.
    pub fn record(&mut self, key: CursorProgressKey) -> bool {
        if self.facts.contains_key(&key) {
            return false;
        }
        self.facts.insert(key, CursorProgressFact::Pending);
        true
    }

    /// Marks every pending boundary at or below `through` consumed for one
    /// participant, without touching another participant's identical boundary.
    pub fn consume_through(
        &mut self,
        participant_index: ParticipantIndex,
        through: DeliverySeq,
    ) -> Vec<CursorProgressKey> {
        let mut consumed = Vec::new();
        for (key, fact) in &mut self.facts {
            if key.participant_index == participant_index
                && key.boundary <= through
                && *fact == CursorProgressFact::Pending
            {
                *fact = CursorProgressFact::Consumed;
                consumed.push(*key);
            }
        }
        consumed
    }

    /// Returns one exact fact state.
    #[must_use]
    pub fn get(&self, key: CursorProgressKey) -> Option<CursorProgressFact> {
        self.facts.get(&key).copied()
    }

    /// Number of distinct participant/boundary pairs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.facts.len()
    }

    /// Returns whether no cursor fact exists.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.facts.is_empty()
    }

    /// Deterministically serializes the variable map in key order.
    ///
    /// Format: `count:u32`, followed by `participant_index:u64`,
    /// `boundary:u64`, and `state:u8` (`0` Pending, `1` Consumed) per fact.
    /// This is lifecycle storage, not the participant network frame format.
    ///
    /// # Errors
    ///
    /// Returns [`CursorFactEncodeError`] if count or allocation length cannot be
    /// represented.
    pub fn encode(&self) -> Result<Vec<u8>, CursorFactEncodeError> {
        let count =
            u32::try_from(self.facts.len()).map_err(|_| CursorFactEncodeError::TooManyFacts)?;
        let body_len = self
            .facts
            .len()
            .checked_mul(17)
            .ok_or(CursorFactEncodeError::LengthOverflow)?;
        let capacity = 4_usize
            .checked_add(body_len)
            .ok_or(CursorFactEncodeError::LengthOverflow)?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(&count.to_be_bytes());
        for (key, fact) in &self.facts {
            bytes.extend_from_slice(&key.participant_index.to_be_bytes());
            bytes.extend_from_slice(&key.boundary.to_be_bytes());
            bytes.push(match fact {
                CursorProgressFact::Pending => 0,
                CursorProgressFact::Consumed => 1,
            });
        }
        Ok(bytes)
    }
}
