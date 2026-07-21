use std::collections::BTreeSet;

use liminal_protocol::wire::{BindingEpoch, ConnectionIncarnation, Generation};

use super::dispatch_impact::{
    DispatchEffect, DispatchImpact, DispatchImpactAccumulator, DispatchTarget,
};

fn target(participant_id: u64, connection_ordinal: u64) -> DispatchTarget {
    DispatchTarget::new(
        participant_id,
        BindingEpoch::new(
            ConnectionIncarnation::new(7, connection_ordinal),
            Generation::ONE,
        ),
    )
}

#[test]
fn empty_accumulator_is_unchanged() {
    let impact = DispatchImpactAccumulator::new().finish(41);

    assert_eq!(impact, DispatchImpact::Unchanged);
    assert_eq!(impact.conversation_id(), None);
    assert!(impact.effects().is_none());
    assert!(impact.target_union().is_empty());
}

#[test]
fn truthful_effect_with_no_current_target_remains_changed() -> Result<(), String> {
    let mut accumulator = DispatchImpactAccumulator::new();
    accumulator.record(DispatchEffect::Retired, []);
    let impact = accumulator.finish(41);

    assert_eq!(impact.conversation_id(), Some(41));
    let effects = impact
        .effects()
        .ok_or_else(|| "recorded effect became unchanged".to_owned())?;
    let retired = effects
        .get(&DispatchEffect::Retired)
        .ok_or_else(|| "Retired effect was dropped".to_owned())?;
    assert!(retired.is_empty());
    assert!(impact.target_union().is_empty());
    Ok(())
}

#[test]
fn repeated_and_prefix_effects_union_without_precedence() -> Result<(), String> {
    let first = target(2, 11);
    let second = target(3, 12);
    let third = target(4, 13);
    let mut accumulator = DispatchImpactAccumulator::new();
    accumulator.record(DispatchEffect::Published, [first]);
    accumulator.record(DispatchEffect::Published, [second]);
    accumulator.record(DispatchEffect::BindingChanged, [second]);

    let mut prefix = DispatchImpactAccumulator::new();
    prefix.record(DispatchEffect::EpisodeChanged, [third]);
    prefix.record(DispatchEffect::Acknowledged, [first, third]);
    accumulator.merge(prefix);

    assert!(!accumulator.is_empty());
    let impact = accumulator.finish(82);
    let effects = impact
        .effects()
        .ok_or_else(|| "merged effects became unchanged".to_owned())?;
    assert_eq!(
        effects.get(&DispatchEffect::Published),
        Some(&BTreeSet::from([first, second]))
    );
    assert_eq!(
        effects.get(&DispatchEffect::BindingChanged),
        Some(&BTreeSet::from([second]))
    );
    assert_eq!(
        effects.get(&DispatchEffect::EpisodeChanged),
        Some(&BTreeSet::from([third]))
    );
    assert_eq!(
        effects.get(&DispatchEffect::Acknowledged),
        Some(&BTreeSet::from([first, third]))
    );
    assert_eq!(
        impact.target_union(),
        BTreeSet::from([first, second, third])
    );
    assert_eq!(first.participant_id(), 2);
    assert_eq!(
        first
            .binding_epoch()
            .connection_incarnation
            .connection_ordinal,
        11
    );
    Ok(())
}
