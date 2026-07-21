//! Typed post-commit tells for participant obligation dispatch.
//!
//! One accumulator belongs to one semantic request while its conversation owner
//! is locked. Producers record effects immediately after each durable subcommit
//! is installed. Finishing the accumulator preserves every committed prefix,
//! including when a later operation refuses or fails.

use std::collections::{BTreeMap, BTreeSet};

use liminal_protocol::wire::{BindingEpoch, ConversationId, ParticipantId};

/// Exhaustive reasons a committed participant operation can change dispatch
/// permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DispatchEffect {
    /// A Produced batch installed at least one live recipient obligation.
    Published,
    /// A normal or marker acknowledgement changed a dispatch cursor or verdict.
    Acknowledged,
    /// A current binding was installed, replaced, detached, or retired.
    BindingChanged,
    /// Coupled closure-debt episode state changed.
    EpisodeChanged,
    /// A permanent `Left` commit discharged one participant.
    Retired,
}

/// Exact poststate binding eligible to be told about changed permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DispatchTarget {
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
}

impl DispatchTarget {
    /// Captures one exact current participant binding.
    #[must_use]
    pub const fn new(participant_id: ParticipantId, binding_epoch: BindingEpoch) -> Self {
        Self {
            participant_id,
            binding_epoch,
        }
    }

    /// Returns the permanent participant identity.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the exact current binding, including connection incarnation.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }
}

/// Complete request-level post-commit dispatch impact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchImpact {
    /// No committed subcommit changed dispatch permission.
    Unchanged,
    /// At least one truthful effect occurred for one conversation.
    Changed {
        /// Conversation whose locked authority produced the effects.
        conversation_id: ConversationId,
        /// Nonempty effect map. Individual truthful effects may have no current
        /// binding target.
        effects: BTreeMap<DispatchEffect, BTreeSet<DispatchTarget>>,
    },
}

impl DispatchImpact {
    /// Returns the changed conversation, if any.
    #[must_use]
    pub const fn conversation_id(&self) -> Option<ConversationId> {
        match self {
            Self::Unchanged => None,
            Self::Changed {
                conversation_id, ..
            } => Some(*conversation_id),
        }
    }

    /// Returns the effect map for a changed impact.
    #[must_use]
    pub const fn effects(&self) -> Option<&BTreeMap<DispatchEffect, BTreeSet<DispatchTarget>>> {
        match self {
            Self::Unchanged => None,
            Self::Changed { effects, .. } => Some(effects),
        }
    }

    /// Unions and deduplicates all exact targets without applying effect
    /// precedence.
    #[must_use]
    pub fn target_union(&self) -> BTreeSet<DispatchTarget> {
        match self {
            Self::Unchanged => BTreeSet::new(),
            Self::Changed { effects, .. } => effects
                .values()
                .flat_map(|targets| targets.iter().copied())
                .collect(),
        }
    }
}

/// Lossless request-scoped accumulator for installed subcommit effects.
#[derive(Debug, Default)]
pub struct DispatchImpactAccumulator {
    effects: BTreeMap<DispatchEffect, BTreeSet<DispatchTarget>>,
    staged_effects: BTreeMap<DispatchEffect, BTreeSet<DispatchTarget>>,
}

impl DispatchImpactAccumulator {
    /// Starts an empty request accumulator.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            effects: BTreeMap::new(),
            staged_effects: BTreeMap::new(),
        }
    }

    /// Records one truthful effect after its durable subcommit is installed.
    ///
    /// Repeated effects union exact target sets. An empty target iterator still
    /// records the effect because lack of a current binding does not erase a
    /// committed permission change.
    pub fn record(
        &mut self,
        effect: DispatchEffect,
        targets: impl IntoIterator<Item = DispatchTarget>,
    ) {
        self.effects.entry(effect).or_default().extend(targets);
    }

    /// Stages an effect whose enclosing durability/reconciliation barrier has
    /// not installed every coupled owner yet.
    pub(crate) fn stage(
        &mut self,
        effect: DispatchEffect,
        targets: impl IntoIterator<Item = DispatchTarget>,
    ) {
        self.staged_effects
            .entry(effect)
            .or_default()
            .extend(targets);
    }

    /// Commits every staged effect after the enclosing barrier is installed.
    pub(crate) fn install_staged(&mut self) {
        let staged = core::mem::take(&mut self.staged_effects);
        for (effect, targets) in staged {
            self.record(effect, targets);
        }
    }

    /// Reports whether a durable source awaits reconciliation before telling.
    pub(crate) fn has_staged(&self) -> bool {
        !self.staged_effects.is_empty()
    }

    /// Merges another installed-prefix accumulator without replacing any
    /// earlier effect or target.
    pub fn merge(&mut self, prefix: Self) {
        for (effect, targets) in prefix.effects {
            self.record(effect, targets);
        }
        for (effect, targets) in prefix.staged_effects {
            self.stage(effect, targets);
        }
    }

    /// Returns whether no committed subcommit recorded an effect.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }

    /// Converts the request accumulator into its externally carried impact.
    #[must_use]
    pub fn finish(self, conversation_id: ConversationId) -> DispatchImpact {
        if self.effects.is_empty() {
            DispatchImpact::Unchanged
        } else {
            DispatchImpact::Changed {
                conversation_id,
                effects: self.effects,
            }
        }
    }
}
